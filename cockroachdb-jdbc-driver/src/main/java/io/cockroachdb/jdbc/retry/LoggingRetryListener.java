package io.cockroachdb.jdbc.retry;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import io.cockroachdb.jdbc.util.ExceptionUtils;

/**
 * Retry listener delegating to a logger.
 */
public class LoggingRetryListener implements RetryListener {
    protected final Logger logger;

    private final Marker marker = MarkerFactory.getMarker("RETRY");

    private final AtomicInteger numRetriesSuccessful = new AtomicInteger();

    private final AtomicInteger numRetriesFailed = new AtomicInteger();

    public LoggingRetryListener() {
        this(LoggerFactory.getLogger(LoggingRetryListener.class));
    }

    public LoggingRetryListener(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void configure(Properties properties) {
    }

    @Override
    public void beforeRetry(String methodName, int attempt, SQLException ex, Duration executionTime) {
        logger.debug(marker,
                "Transaction retry started: attempt [{}] for method [{}] with execution time [{}]\n{}",
                attempt, methodName, executionTime, ExceptionUtils.toNestedString(ex));
    }

    @Override
    public void afterRetry(String methodName, int attempt, SQLException ex,
                           Duration executionTime) {
        if (ex != null) {
            logger.warn(marker,
                    "Transaction retry failed: attempt [{}] for method [{}] with execution time [{}]\n{}",
                    attempt, methodName, executionTime, ExceptionUtils.toNestedString(ex));
            numRetriesFailed.incrementAndGet();
        } else {
            logger.info(marker,
                    "Transaction retry successful: attempt [{}] for method [{}] with execution time [{}]",
                    attempt, methodName, executionTime);
            numRetriesSuccessful.incrementAndGet();
        }
    }

    public int getSuccessfulRetries() {
        return numRetriesSuccessful.get();
    }

    public int getFailedRetries() {
        return numRetriesFailed.get();
    }
}

