package io.cockroachdb.jdbc.it.basic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;
import org.postgresql.ds.PGSimpleDataSource;

import com.zaxxer.hikari.HikariDataSource;

import io.cockroachdb.jdbc.CockroachDataSource;
import io.cockroachdb.jdbc.CockroachDriver;
import io.cockroachdb.jdbc.CockroachProperty;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

@Disabled
public class PlainJdbcTest {
    private static DataSource createDataSource() throws Exception {
        return createCockroachDataSource();
    }

    private static DataSource createPostgreSQLDataSource() throws Exception {
        Class.forName(CockroachDriver.class.getName());

        PGSimpleDataSource ds = new PGSimpleDataSource();

        ConnectionDetails details = ConnectionDetails.getInstance();
        ds.setUrl(details.getUrl());
        ds.setUser(details.getUser());
        ds.setPassword(details.getPassword());

        return loggingProxy(ds);
    }

    private static DataSource createCockroachDataSource() throws Exception {
        Class.forName(CockroachDriver.class.getName());

        CockroachDataSource ds = new CockroachDataSource();

        ConnectionDetails details = ConnectionDetails.getInstance();
        ds.setUrl(details.getUrl());
        ds.setUsername(details.getUser());
        ds.setPassword(details.getPassword());
        ds.setAutoCommit(true);

        ds.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");

        ds.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), "true");
        ds.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), "true");

        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "5");
        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15s");

        return loggingProxy(ds);
    }

    private static DataSource pooledDataSource() throws Exception {
        Class.forName(CockroachDriver.class.getName());

        HikariDataSource ds = new HikariDataSource();

        ConnectionDetails details = ConnectionDetails.getInstance();
        ds.setJdbcUrl(details.getUrl());
        ds.setUsername(details.getUser());
        ds.setPassword(details.getPassword());

        ds.setMaximumPoolSize(5);
        ds.setMinimumIdle(5);
        ds.setAutoCommit(true);

        ds.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");

        ds.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), "true");
        ds.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), "true");

        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "5");
        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15s");

        return loggingProxy(ds);
    }

    private static DataSource loggingProxy(DataSource ds) {
        return ProxyDataSourceBuilder
                .create(ds)
                .logQueryBySlf4j(SLF4JLogLevel.TRACE, "SQL_TRACE")
//                .multiline()
                .asJson()
                .build();
    }

    @FunctionalInterface
    private interface SQLCallable<T> {
        T call() throws SQLException;
    }

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(8);

    private <T> Future<T> executeAsync(SQLCallable<T> c) {
        return executorService.submit(() -> c.call());
    }

    private <T> Future<T> executeAsync(SQLCallable<T> c, int delaySeconds) {
        return executorService.schedule(() -> c.call(), delaySeconds, TimeUnit.SECONDS);
    }

    @BeforeEach
    public void setupSchema() throws Exception {
        DataSource dataSource = createDataSource();

        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            c.createStatement().execute("create table if not exists test (id int, value int)");
            c.createStatement().execute("delete from test where 1 = 1");
            c.createStatement().execute("insert into test (id, value) values (1, 10), (2, 20)");
        }
    }

    @Test
    public void testWriteReadConflict() throws Exception {
        DataSource dataSource = createDataSource();

        Connection t1 = dataSource.getConnection();
        t1.setAutoCommit(false);
        t1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Connection t2 = dataSource.getConnection();
        t2.setAutoCommit(false);
        t2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        PreparedStatement ps = t1.prepareStatement("update test set value=11 where id=1");
        ps.executeUpdate();

        Future<Integer> f1 = executeAsync(() -> {
            PreparedStatement ps2 = t2.prepareStatement("update test set value=22 where id=2");
            return ps2.executeUpdate();
        });

        PreparedStatement ps3 = t1.prepareStatement("select * from test where id=2");
        ps3.executeQuery();

        Future<ResultSet> f2 = executeAsync(() -> {
            PreparedStatement ps4 = t2.prepareStatement("select * from test where id=1");
            return ps4.executeQuery();
        });

        t1.commit();

        Assertions.assertEquals(1, f1.get());

        Assertions.assertEquals(true, f2.get().next());
        Assertions.assertEquals(1, f2.get().getInt(1));
        Assertions.assertEquals(11, f2.get().getInt(2));
        Assertions.assertEquals(false, f2.get().next());

        t2.commit();
    }

    @Test
    public void testWriteSkewConflict() throws Exception {
        DataSource dataSource = createDataSource();

        Connection t1 = dataSource.getConnection();
        t1.setAutoCommit(false);
        t1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Connection t2 = dataSource.getConnection();
        t2.setAutoCommit(false);
        t2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        try (PreparedStatement p1 = t1.prepareStatement("select * from test where id in (1,2)")) {
            try (ResultSet rs = p1.executeQuery()) {
                Assertions.assertEquals(true, rs.next());
                Assertions.assertEquals(1, rs.getInt(1));
                Assertions.assertEquals(10, rs.getInt(2));
                Assertions.assertEquals(true, rs.next());
                Assertions.assertEquals(2, rs.getInt(1));
                Assertions.assertEquals(20, rs.getInt(2));
                Assertions.assertEquals(false, rs.next());
            }
        }

        executeAsync(() -> {
            try (PreparedStatement p2 = t2.prepareStatement("select * from test where id in (1,2)")) {
                try (ResultSet rs = p2.executeQuery()) {
                    Assertions.assertEquals(true, rs.next());
                    Assertions.assertEquals(1, rs.getInt(1));
                    Assertions.assertEquals(10, rs.getInt(2));
                    Assertions.assertEquals(true, rs.next());
                    Assertions.assertEquals(2, rs.getInt(1));
                    Assertions.assertEquals(20, rs.getInt(2));
                    Assertions.assertEquals(false, rs.next());
                }
            }
            return null;
        });

        try (PreparedStatement u1 = t1.prepareStatement("update test set value = 11 where id = 1")) {
            u1.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        executeAsync(() -> {
            t1.commit();
            t1.close();
            return null;
        }, 2);

        try (PreparedStatement u2 = t2.prepareStatement("update test set value = 22 where id = 2")) {
            u2.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        t2.commit();

        try (Connection t3 = dataSource.getConnection()) {
            t3.setAutoCommit(false);
            PreparedStatement p4 = t3.prepareStatement("select * from test where id in (1,2)");
            try (ResultSet rs = p4.executeQuery()) {
                Assertions.assertEquals(true, rs.next());
                Assertions.assertEquals(1, rs.getInt(1));
                Assertions.assertEquals(11, rs.getInt(2));
                Assertions.assertEquals(true, rs.next());
                Assertions.assertEquals(2, rs.getInt(1));
                Assertions.assertEquals(22, rs.getInt(2));
                Assertions.assertEquals(false, rs.next());
            }
            t3.commit();
        }

        t1.close();
        t2.close();
    }
}
