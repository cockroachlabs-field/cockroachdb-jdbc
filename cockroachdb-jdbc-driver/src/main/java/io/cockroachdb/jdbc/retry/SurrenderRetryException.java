package io.cockroachdb.jdbc.retry;

import java.sql.SQLException;

import io.cockroachdb.jdbc.CockroachException;
import io.cockroachdb.jdbc.util.ExceptionUtils;

public class SurrenderRetryException extends CockroachException {
    public SurrenderRetryException(String reason, SQLException cause) {
        super("The transaction could not proceed due to: " + reason,
                ExceptionUtils.toPSQLState(cause.getSQLState()), cause);
    }
}
