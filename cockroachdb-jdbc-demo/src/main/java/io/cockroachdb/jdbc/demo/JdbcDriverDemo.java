package io.cockroachdb.jdbc.demo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import io.cockroachdb.jdbc.CockroachDriver;
import io.cockroachdb.jdbc.CockroachProperty;
import io.cockroachdb.jdbc.retry.EmptyRetryListener;
import io.cockroachdb.jdbc.util.ExceptionUtils;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class JdbcDriverDemo {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDriverDemo.class);

    private int concurrency = Runtime.getRuntime().availableProcessors() * 2;

    private boolean printErrors;

    boolean clientRetry;

    /**
     * Run the demo workload using the given datasource.
     *
     * @param ds the datasource
     */
    public void run(DataSource ds) {
        String version = JdbcUtils.select(ds, "select version()", resultSet -> {
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
            return null;
        });

        logger.info("Connected to \"{}\"", version);

        final List<Long> systemAccountIDs = findSystemAccountIDs(ds);
        if (systemAccountIDs.isEmpty()) {
            throw new IllegalStateException("No system accounts found!");

        }
        final List<Long> userAccountIDs = findUserAccountIDs(ds);
        logger.info("Found {} system and {} user accounts - scheduling workers for each", systemAccountIDs.size(),
                userAccountIDs.size());

        // Unbounded thread pool is fine since the connection pool will throttle
        final ExecutorService unboundedPool = Executors.newScheduledThreadPool(concurrency);

        final Deque<Future<?>> futures = new ArrayDeque<>();

        systemAccountIDs.forEach(systemAccountId -> {
            userAccountIDs.forEach(userAccountId -> {
                futures.add(unboundedPool.submit(() -> {
                    BigDecimal amount = new BigDecimal("100.00");
                    List<AccountLeg> legs = new ArrayList<>();
                    legs.add(new AccountLeg(systemAccountId, amount.negate()));
                    legs.add(new AccountLeg(userAccountId, amount));
                    if (clientRetry) {
                        return JdbcUtils.executeWithinTransactionWithRetry(ds, transferFunds(legs));
                    } else {
                        return JdbcUtils.executeWithinTransaction(ds, transferFunds(legs));
                    }
                }));
            });
        });

        logger.info("Scheduled {} futures", futures.size());

        final int total = futures.size();
        int commits = 0;
        int rollbacks = 0;
        int violations = 0;

        final Instant callTime = Instant.now();

        while (!futures.isEmpty()) {
            if (futures.size() % 100 == 0) {
                logger.info("Awaiting completion ({} commits, {} rollbacks, {} violations, {} remaining)", commits,
                        rollbacks, violations, futures.size());
            }
            try {
                futures.pop().get();
                commits++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                Throwable throwable = ExceptionUtils.getMostSpecificCause(e);
                if (throwable instanceof BusinessException) {
                    violations++;
                } else {
                    rollbacks++;
                }
                if (printErrors) {
                    logger.warn(throwable.toString());
                }
            }
        }

        unboundedPool.shutdownNow();

        System.out.printf("\u001B[36m%,d commits\u001B[0m\n", commits);
        System.out.printf("\u001B[36m%,d rollbacks\u001B[0m\n", rollbacks);
        System.out.printf("\u001B[36m%.2f%% commit rate! %s\u001B[0m\n",
                100 - (rollbacks / (double) (Math.max(1, total))) * 100.0,
                rollbacks > 0 ? "(╯°□°)╯︵ ┻━┻" : "¯\\\\_(ツ)_/¯");

        System.out.printf("\u001B[36m%,d rule violations\u001B[0m\n", violations);
        System.out.printf("\u001B[36m%.2f%% violation rate! %s\u001B[0m\n",
                (violations / (double) (Math.max(1, total))) * 100.0, violations > 0 ? "(╯°□°)╯︵ ┻━┻" : "¯\\\\_(ツ)_/¯");

        System.out.printf("\u001B[33m%s execution time\u001B[0m\n", Duration.between(callTime, Instant.now()));
        System.out.printf("\u001B[33m%.2f avg TPS\u001B[0m\n",
                ((double) total / (double) Duration.between(callTime, Instant.now()).toSeconds()));
    }

    private List<Long> findUserAccountIDs(DataSource ds) {
        return JdbcUtils.select(ds, "SELECT id,type FROM bank_account WHERE type='U' ORDER BY id", resultSet -> {
            List<Long> ids = new ArrayList<>();
            while (resultSet.next()) {
                ids.add(resultSet.getLong(1));
            }
            return ids;
        });
    }

    private List<Long> findSystemAccountIDs(DataSource ds) {
        return JdbcUtils.select(ds, "SELECT id,type FROM bank_account WHERE type='S' ORDER BY id", resultSet -> {
            List<Long> ids = new ArrayList<>();
            while (resultSet.next()) {
                ids.add(resultSet.getLong(1));
            }
            return ids;
        });
    }

    private ConnectionCallback<Void> transferFunds(List<AccountLeg> legs) {
        return conn -> {
            BigDecimal checksum = BigDecimal.ZERO;

            for (AccountLeg leg : legs) {
                BigDecimal balance = readBalance(conn, leg.getId());
                BigDecimal newBalance = balance.add(leg.getAmount());
                if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
                    throw new BusinessException(
                            "Negative balance outcome " + newBalance.toPlainString() + " for account ID "
                                    + leg.getId());
                }
                updateBalance(conn, leg.getId(), newBalance);
                checksum = checksum.add(leg.getAmount());
            }

            if (checksum.compareTo(BigDecimal.ZERO) != 0) {
                throw new BusinessException(
                        "Sum of account legs must equal zero (got " + checksum.toPlainString() + ")");
            }

            return null;
        };
    }

    private BigDecimal readBalance(Connection conn, Long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT balance FROM bank_account WHERE id = ?")) {
            ps.setLong(1, id);

            try (ResultSet res = ps.executeQuery()) {
                if (!res.next()) {
                    throw new DataAccessException("Account not found: " + id);
                }
                return res.getBigDecimal("balance");
            }
        }
    }

    private void updateBalance(Connection conn, Long id, BigDecimal balance) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE bank_account SET balance = ?, updated_at=clock_timestamp() WHERE id = ?")) {
            ps.setBigDecimal(1, balance);
            ps.setLong(2, id);
            if (ps.executeUpdate() != 1) {
                throw new DataAccessException("Rows affected != 1  for " + id);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:cockroachdb://localhost:26257/jdbc_test?sslmode=disable";

        String username = "root";

        String password = "";

        boolean traceSQL = false;

        boolean sfu = false;

        boolean driverRetry = false;

        boolean clientRetry = false;

        int concurrency = Runtime.getRuntime().availableProcessors() * 2;

        boolean printErrors = false;

        boolean printHelpAndQuit = false;

        LinkedList<String> argsList = new LinkedList<>(Arrays.asList(args));
        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.startsWith("--concurrency")) {
                concurrency = Integer.parseInt(argsList.pop());
            } else if (arg.startsWith("--url")) {
                url = argsList.pop();
            } else if (arg.startsWith("--username")) {
                username = argsList.pop();
            } else if (arg.startsWith("--password")) {
                password = argsList.pop();
            } else if (arg.startsWith("--trace")) {
                traceSQL = true;
            } else if (arg.startsWith("--sfu")) {
                sfu = true;
            } else if (arg.startsWith("--driverRetry")) {
                driverRetry = true;
            } else if (arg.startsWith("--clientRetry")) {
                clientRetry = true;
            } else if (arg.startsWith("--printErrors")) {
                printErrors = true;
            } else if (arg.startsWith("--")) {
                System.out.println("Unrecognized option: " + arg);
                printHelpAndQuit = true;
                break;
            } else {
                System.out.println("Unrecognized argument: " + arg);
                printHelpAndQuit = true;
                break;
            }
        }

        if (printHelpAndQuit) {
            System.out.println("Usage: java -jar cockroachdb-jdbc-demo.jar [options]");
            System.out.println("Options include: (defaults in parenthesis)");
            System.out.println("--sfu               enable implicit SFU (false)");
            System.out.println("--driverRetry       enable JDBC driver transaction retries (false)");
            System.out.println("--clientRetry       enable client/app transaction retries (false)");
            System.out.println("--trace             enable SQL tracing (false)");
            System.out.println("--printErrors       print exceptions to console (false)");
            System.out.println("--concurrency N     number of threads (system x 2)");
            System.out.println(
                    "--url <url>         JDBC connection URL (jdbc:cockroachdb://localhost:26257/jdbc_test)");
            System.out.println("--username <user>   JDBC login user name (root)");
            System.out.println("--password <secret> JDBC login password (empty)");
            System.exit(0);
        }

        final HikariDataSource hikariDS = new HikariDataSource();
        hikariDS.setJdbcUrl(url);
        hikariDS.setDriverClassName(CockroachDriver.class.getName());
        hikariDS.setUsername(username);
        hikariDS.setPassword(password);
        hikariDS.setAutoCommit(true);
        hikariDS.setMaximumPoolSize(concurrency);
        hikariDS.setMinimumIdle(concurrency / 2);
        hikariDS.setMaxLifetime(TimeUnit.MINUTES.toMillis(3));
        hikariDS.setIdleTimeout(TimeUnit.MINUTES.toMillis(1));
        hikariDS.setConnectionTimeout(TimeUnit.MINUTES.toMillis(1));
        hikariDS.setInitializationFailTimeout(-1);

        hikariDS.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), sfu);
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), driverRetry);
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "30");
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15000");
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_LISTENER_CLASSNAME.getName(),
                EmptyRetryListener.class.getName());

        hikariDS.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");

        DataSource ds = traceSQL ? ProxyDataSourceBuilder.create(hikariDS).asJson()
                .logQueryBySlf4j(SLF4JLogLevel.TRACE, "io.cockroachdb.jdbc.SQL_TRACE").multiline().build() : hikariDS;

        SchemaSupport.setupSchema(ds);

        JdbcDriverDemo demo = new JdbcDriverDemo();
        demo.concurrency = concurrency;
        demo.printErrors = printErrors;
        demo.clientRetry = clientRetry;
        demo.run(ds);
    }
}
