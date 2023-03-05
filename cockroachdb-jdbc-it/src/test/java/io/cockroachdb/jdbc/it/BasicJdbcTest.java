package io.cockroachdb.jdbc.it;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("basic")
public class BasicJdbcTest extends AbstractIntegrationTest {
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
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            c.createStatement().execute("create table if not exists test (id int, value int)");
            c.createStatement().execute("delete from test where 1 = 1");
            c.createStatement().execute("insert into test (id, value) values (1, 10), (2, 20)");
        }
    }

    @Test
    public void testWriteReadConflict() throws Exception {
        Connection t1 = dataSource.getConnection();
        t1.setAutoCommit(false);

        Connection t2 = dataSource.getConnection();
        t2.setAutoCommit(false);

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
        Connection t1 = dataSource.getConnection();
        t1.setAutoCommit(false);

        Connection t2 = dataSource.getConnection();
        t2.setAutoCommit(false);

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
