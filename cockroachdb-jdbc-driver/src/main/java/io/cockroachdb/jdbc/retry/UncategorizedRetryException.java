package io.cockroachdb.jdbc.retry;

import java.sql.SQLException;

import org.postgresql.util.PSQLState;

import io.cockroachdb.jdbc.CockroachException;
import io.cockroachdb.jdbc.util.ExceptionUtils;

public class UncategorizedRetryException extends CockroachException {
    public UncategorizedRetryException(String reason) {
        super("The transaction could not proceed due to: " + reason,
                PSQLState.UNKNOWN_STATE);
    }

    public UncategorizedRetryException(String reason, SQLException cause) {
        super("The transaction could not proceed due to: " + reason,
                ExceptionUtils.toPSQLState(cause.getSQLState()), cause);
    }
}
