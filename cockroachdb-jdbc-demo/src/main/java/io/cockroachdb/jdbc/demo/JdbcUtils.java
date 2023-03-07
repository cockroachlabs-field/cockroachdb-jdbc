package io.cockroachdb.jdbc.demo;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

public abstract class JdbcUtils {
    private JdbcUtils() {
    }

    public static <T> T select(DataSource dataSource, String sql,
                               ResultSetCallback<T> action)
            throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    return action.process(rs);
                }
            }
        }
    }

    public static int update(DataSource dataSource, String sql, Object... params)
            throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                int idx = 1;
                for (Object p : params) {
                    ps.setObject(idx++, p);
                }
                return ps.executeUpdate();
            }
        }
    }

    public static <T> T executeWithoutTransaction(DataSource ds,
                                                  ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            T result;
            try {
                result = action.process(conn);
            } catch (RuntimeException | Error ex) {
                throw ex;
            } catch (Throwable ex) {
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    public static <T> T executeWithinTransaction(DataSource ds,
                                                 ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            T result;
            try {
                result = action.process(conn);
            } catch (RuntimeException | Error ex) {
                conn.rollback();
                throw ex;
            } catch (Throwable ex) {
                conn.rollback();
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
            conn.commit();
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }
}
