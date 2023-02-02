package io.cockroachdb.jdbc.retry;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Interface specifying the API to be implemented by a class providing
 * a retry strategy in terms of exception classification.
 *
 * <p>This is intended for internal use by the CockroachDB JDBC driver.
 * See {@link ExponentialBackoffRetryStrategy}.
 */
public interface RetryStrategy {
    /**
     * Configure the strategy, implementation specific.
     *
     * @param properties the configuration properties, optionally provided in JDBC URL
     */
    default void configure(Properties properties) {
    }

    default String getDescription() {
        return toString();
    }

    /**
     * Classification of exceptions as connection failures or not. Connection errors are
     * potentially recoverable and a retry may be permitted or not depending on strategy.
     *
     * @param ex the exception to inspect
     * @return true if the exception denotes a connection error and false if not
     */
    boolean isConnectionError(SQLException ex);

    /**
     * Classification of exceptions as retryable or not. Typically, the supplied
     * exception is an optimistic or pessimistic lock exception or some other
     * data access exception considered transient - where a previously failed
     * operation might succeed when the operation is retried without any intervention
     * by application-level functionality.
     *
     * @param ex the exception to inspect
     * @return true if the exception is retryable and false if not
     */
    boolean isRetryableException(SQLException ex);

    /**
     * Determine if a retry attempt should proceed or not. This method should only be invoked
     * after an exception is classified as retryable.
     *
     * @param attempt the retry attempt number, 1-based
     * @param startTime point in time when the initial invocation failed with a retryable exception
     * @return true to proceed, false otherwise
     */
    boolean proceedWithRetry(int attempt, Instant startTime);

    /**
     * An implementation may choose to suspend the calling thread for an unspecified period of time.
     *
     * @param attempt the retry attempt number, 1-based
     * @param startTime point in time when the initial invocation failed with a retryable exception
     * @param callback callback function for the delay interval invoked before to the actual wait
     */
    void waitBeforeRetry(int attempt, Instant startTime, Consumer<Duration> callback);
}
