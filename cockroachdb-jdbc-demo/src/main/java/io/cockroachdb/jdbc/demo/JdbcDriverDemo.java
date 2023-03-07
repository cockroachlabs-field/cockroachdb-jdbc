package io.cockroachdb.jdbc.demo;

import java.lang.reflect.UndeclaredThrowableException;
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
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class JdbcDriverDemo {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDriverDemo.class);

    private int concurrency;

    private boolean printErrors;

    /**
     * Run the demo workload using the given datasource.
     *
     * @param ds the datasource
     * @throws SQLException on any errors
     */
    public void run(DataSource ds) throws SQLException {
        String version = JdbcUtils.select(ds, "select version()", resultSet -> {
            if (resultSet.next()) {
                return resultSet.getString(1);
            }
            return null;
        });

        logger.info("Connected to \"{}\"", version);

        final ExecutorService unboundedPool = Executors.newScheduledThreadPool(concurrency);
        final Deque<Future<?>> futures = new ArrayDeque<>();

        final List<Long> systemAccounts =
                JdbcUtils.select(ds, "SELECT id,account_type FROM account WHERE account_type='S' ORDER BY id",
                        resultSet -> {
                            List<Long> ids = new ArrayList<>();
                            while (resultSet.next()) {
                                ids.add(resultSet.getLong(1));
                            }
                            return ids;
                        });

        if (systemAccounts.isEmpty()) {
            throw new IllegalStateException("No system accounts found!");
        }

        logger.info("Found {} system accounts - scheduling workers", systemAccounts.size());

        JdbcUtils.select(ds, "SELECT id,account_type FROM account WHERE account_type='U' ORDER BY id", resultSet -> {
            final BigDecimal amount = new BigDecimal("100.00");
            while (resultSet.next()) {
                Long id = resultSet.getLong(1);
                futures.add(unboundedPool.submit(() -> {
                            List<AccountLeg> legs = new ArrayList<>();
                            legs.add(new AccountLeg(systemAccounts.get(0), amount.negate()));
                            legs.add(new AccountLeg(id, amount));
                            return JdbcUtils.executeWithinTransaction(ds, transferFunds(legs));
                        }
                ));
            }
            logger.info("Scheduled {} workers", futures.size());
            return null;
        });

        int commits = 0;
        int rollbacks = 0;

        final Instant callTime = Instant.now();

        while (!futures.isEmpty()) {
            if (futures.size() % 10 == 0) {
                logger.info("Awaiting completion ({} commits {} rollbacks {} remaining)",
                        commits, rollbacks, futures.size());
            }
            try {
                futures.pop().get();
                commits++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                rollbacks++;
                if (printErrors) {
                    if (e.getCause() instanceof UndeclaredThrowableException) {
                        Throwable throwable = ((UndeclaredThrowableException) e.getCause()).getUndeclaredThrowable();
                        logger.warn(throwable.toString());
                    } else {
                        logger.warn(e.getCause().toString());
                    }
                }
            }
        }

        unboundedPool.shutdownNow();

        System.out.printf("\u001B[33mCommits (%d) Rollbacks (%d) Commit Rate (%.2f%%)\u001B[36m\n%s\u001B[0m\nRuntime %s\n",
                commits,
                rollbacks,
                100 - (rollbacks / (double) (Math.max(1, commits + rollbacks))) * 100.0,
                rollbacks > 0 ? "(╯°□°)╯︵ ┻━┻" : "¯\\\\_(ツ)_/¯",
                Duration.between(callTime, Instant.now()));
    }

    private ConnectionCallback<Void> transferFunds(List<AccountLeg> legs) {
        return conn -> {
            BigDecimal checksum = BigDecimal.ZERO;

            for (AccountLeg leg : legs) {
                BigDecimal balance = readBalance(conn, leg.getId());

                BigDecimal newBalance = balance.add(leg.getAmount());

                if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
                    throw new DataAccessException(
                            "Negative balance outcome " + newBalance.toPlainString()
                                    + " for account ID " + leg.getId());
                }
                updateBalance(conn, leg.getId(), newBalance);
                checksum = checksum.add(leg.getAmount());
            }

            if (checksum.compareTo(BigDecimal.ZERO) != 0) {
                throw new DataAccessException(
                        "Sum of account legs must equal zero (got " + checksum.toPlainString() + ")"
                );
            }

            return null;
        };
    }

    private BigDecimal readBalance(Connection conn, Long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT balance FROM account WHERE id = ?")) {
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
        try (PreparedStatement ps = conn
                .prepareStatement(
                        "UPDATE account SET balance = ?, updated_at=clock_timestamp() WHERE id = ?")) {
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

        boolean retry = false;

        int concurrency = Runtime.getRuntime().availableProcessors() * 2;

        boolean printErrors = false;

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
            } else if (arg.startsWith("--retry")) {
                retry = true;
            } else if (arg.startsWith("--printErrors")) {
                printErrors = true;
            } else {
                System.out.println("Usage: java -jar cockroachdb-jdbc-demo.jar [options]");
                System.out.println("Options include: (defaults in parenthesis)");
                System.out.println("--sfu               enable implicit SFU (false)");
                System.out.println("--retry             enable driver retry (false)");
                System.out.println("--trace             enable SQL tracing (false)");
                System.out.println("--concurrency N     number of threads (system x 2)");
                System.out.println(
                        "--url <url>         JDBC connection URL (jdbc:cockroachdb://localhost:26257/jdbc_test)");
                System.out.println("--username <user>   JDBC login user name (root)");
                System.out.println("--password <secret> JDBC login password (empty)");
                System.exit(0);
            }
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
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), retry);
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "30");
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15000");

        hikariDS.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");

        DataSource ds = traceSQL ?
                ProxyDataSourceBuilder
                        .create(hikariDS)
                        .asJson()
                        .logQueryBySlf4j(SLF4JLogLevel.TRACE, "io.cockroachdb.jdbc.SQL_TRACE")
                        .multiline()
                        .build()
                : hikariDS;

        SchemaSupport.setupSchema(ds);

        JdbcDriverDemo demo = new JdbcDriverDemo();
        demo.concurrency = concurrency;
        demo.printErrors = printErrors;
        demo.run(ds);
    }
}
