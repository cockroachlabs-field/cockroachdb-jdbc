package io.cockroachdb.jdbc.retry;

import org.postgresql.util.PSQLState;

import io.cockroachdb.jdbc.CockroachException;

public class ConcurrentUpdateException extends CockroachException {
    public ConcurrentUpdateException(String reason) {
        super(reason, PSQLState.SERIALIZATION_FAILURE);
    }
}
