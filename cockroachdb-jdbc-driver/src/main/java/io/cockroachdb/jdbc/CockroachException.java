package io.cockroachdb.jdbc;

import java.sql.SQLException;

import org.postgresql.util.PSQLState;

/**
 * A CockroachDB JDBC driver specific SQL exception type.
 */
public class CockroachException extends SQLException {
    public CockroachException(String msg, PSQLState state) {
        super(msg, state == null ? null : state.getState());
    }

    public CockroachException(String msg,
                              PSQLState state,
                              Throwable cause) {
        super(msg, state == null ? null : state.getState(), cause);
    }
}
