package io.cockroachdb.jdbc.demo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import javax.sql.DataSource;

import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import io.cockroachdb.jdbc.CockroachDriver;
import io.cockroachdb.jdbc.CockroachProperty;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class SimpleJdbcDemo {
    private static final Logger logger = LoggerFactory.getLogger(SimpleJdbcDemo.class);

    private static ConnectionCallback<BigDecimal> transfer(List<AccountLeg> legs) {
        return conn -> {
            BigDecimal checksum = BigDecimal.ZERO;

            for (AccountLeg leg : legs) {
                BigDecimal balance = readBalance(conn, leg.getId());
                balance = balance.add(leg.getAmount());

                if (balance.compareTo(BigDecimal.ZERO) < 0) {
                    throw new DataAccessException(
                            "Negative balance outcome for account ID " + leg.getId());
                }
                updateBalance(conn, leg.getId(), balance.add(leg.getAmount()));
                checksum = checksum.add(leg.getAmount());
            }

            if (checksum.compareTo(BigDecimal.ZERO) != 0) {
                throw new DataAccessException(
                        "Sum of account legs must equal 0 (got " + checksum.toPlainString() + ")"
                );
            }

            return checksum;
        };
    }

    private static BigDecimal readBalance(Connection conn, Long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT balance FROM account WHERE id = ?")) {
            ps.setLong(1, id);

            try (ResultSet res = ps.executeQuery()) {
                if (!res.next()) {
                    throw new DataAccessException("Account not found: " + id);
                }
                return res.getBigDecimal("balance");
            }
        }
    }

    private static void updateBalance(Connection conn, Long id, BigDecimal balance) throws SQLException {
        try (PreparedStatement ps = conn
                .prepareStatement(
                        "UPDATE account SET balance = ?, updated=clock_timestamp() where id = ?")) {
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
        int concurrency = Runtime.getRuntime().availableProcessors() * 2;
        boolean traceSQL = false;
        boolean sfu = false;
        boolean retry = false;

        LinkedList<String> argsList = new LinkedList<>(Arrays.asList(args));
        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.startsWith("--concurrency=")) {
                concurrency = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--url=")) {
                url = arg.split("=")[1];
            } else if (arg.startsWith("--username=")) {
                username = arg.split("=")[1];
            } else if (arg.startsWith("--password=")) {
                password = arg.split("=")[1];
            } else if (arg.startsWith("--trace")) {
                traceSQL = true;
            } else if (arg.startsWith("--sfu")) {
                sfu = true;
            } else if (arg.startsWith("--retry")) {
                retry = true;
            } else {
                System.out.println("Usage: java -jar jdbc-demo.jar [options]");
                System.out.println("Options include:");
                System.out.println("--sfu               enable implicit SFU");
                System.out.println("--retry             enable driver retry");
                System.out.println("--trace             enable SQL tracing");
                System.out.println("--concurrency=N     number of threads");
                System.out.println("--url=<jdbc-url>    JDBC connection URL");
                System.out.println("--username=<user>   JDBC user name");
                System.out.println("--password=<secret> JDBC password");
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
        hikariDS.setMinimumIdle(concurrency);

        hikariDS.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");

        hikariDS.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), sfu);
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), retry);
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "30");
        hikariDS.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15000");

        DataSource ds = traceSQL ?
                ProxyDataSourceBuilder
                        .create(hikariDS)
                        .asJson()
                        .logQueryBySlf4j(SLF4JLogLevel.TRACE, "io.cockroachdb.jdbc.SQL_TRACE")
                        .multiline()
                        .build()
                : hikariDS;

        SchemaSupport.setupSchema(ds);

        final List<Long> userAccounts = new ArrayList<>();
        final List<Long> systemAccounts = new ArrayList<>();

        JdbcUtils.select(ds, "select id,account_type from account order by id", resultSet -> {
            try {
                while (resultSet.next()) {
                    String type = resultSet.getString(2);
                    if ("S".equalsIgnoreCase(type)) {
                        systemAccounts.add(resultSet.getLong(1));
                    } else {
                        userAccounts.add(resultSet.getLong(1));
                    }
                }
            } catch (SQLException e) {
                throw new DataAccessException(e);
            }
        });

        final ExecutorService executorService = Executors.newScheduledThreadPool(concurrency);
        final Deque<Future<?>> futures = new ArrayDeque<>();
        final BigDecimal amount = new BigDecimal("100.00");

        logger.info("Starting demo: {} system accounts, {} user accounts, {} concurrent workers",
                systemAccounts.size(), userAccounts.size(), concurrency);

        userAccounts.forEach(userAccountId -> futures.add(executorService.submit(() -> {
                    List<AccountLeg> legs = new ArrayList<>();
                    legs.add(new AccountLeg(systemAccounts.get(0), amount.negate()));
                    legs.add(new AccountLeg(userAccountId, amount));
                    return TransactionTemplate.executeWithinTransaction(ds, transfer(legs));
                }
        )));

        int commits = 0;
        int rollbacks = 0;

        while (!futures.isEmpty()) {
            try {
                futures.pop().get();
                commits++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                rollbacks++;
//                if (e.getCause() instanceof UndeclaredThrowableException) {
//                    Throwable throwable = ((UndeclaredThrowableException)e.getCause()).getUndeclaredThrowable();
//                    logger.warn(throwable.toString());
//                } else {
//                    logger.warn(e.getCause().toString());
//                }
            } finally {
                System.out.printf("Awaiting completion (%d commits %d rollbacks %d remaining)\n",
                        commits, rollbacks, futures.size());
            }
        }

        executorService.shutdownNow();

        System.out.printf("Summary: %s\n", commitRate(commits, rollbacks));
        System.out.printf("¯\\\\_(ツ)_/¯\n");
    }

    private static String commitRate(int success, int failures) {
        return String.format("\u001B[33mCommits: %d Rollbacks: %d Commit Rate: %.2f%%\u001B[0m",
                success,
                failures,
                100 - (failures / (double) (Math.max(1, success + failures))) * 100.0);
    }
}
