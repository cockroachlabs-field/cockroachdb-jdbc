package io.cockroachdb.jdbc.demo;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class TransactionTemplate {
    public static <T> T executeWithinTransaction(DataSource ds,
                                                 ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            T result;
            try {
                result = action.doInConnection(conn);
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
