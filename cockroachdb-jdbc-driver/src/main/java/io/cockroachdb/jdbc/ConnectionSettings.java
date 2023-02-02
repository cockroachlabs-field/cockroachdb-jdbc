package io.cockroachdb.jdbc;

import io.cockroachdb.jdbc.query.QueryProcessor;
import io.cockroachdb.jdbc.retry.MethodTraceLogger;
import io.cockroachdb.jdbc.retry.RetryListener;
import io.cockroachdb.jdbc.retry.RetryStrategy;

/**
 * Value object for JDBC connection settings.
 */
@SuppressWarnings("UnusedReturnValue")
public class ConnectionSettings {
    private boolean useCockroachMetadata;

    private boolean maskSQLTrace;

    private QueryProcessor queryProcessor;

    private RetryStrategy retryStrategy;

    private RetryListener retryListener;

    private MethodTraceLogger methodTraceLogger;

    public MethodTraceLogger getMethodTraceLogger() {
        return methodTraceLogger;
    }

    public void setMethodTraceLogger(MethodTraceLogger methodTraceLogger) {
        this.methodTraceLogger = methodTraceLogger;
    }

    public boolean isMaskSQLTrace() {
        return maskSQLTrace;
    }

    public void setMaskSQLTrace(boolean maskSQLTrace) {
        this.maskSQLTrace = maskSQLTrace;
    }

    public boolean isUseCockroachMetadata() {
        return useCockroachMetadata;
    }

    public ConnectionSettings setUseCockroachMetadata(boolean useCockroachMetadata) {
        this.useCockroachMetadata = useCockroachMetadata;
        return this;
    }

    public QueryProcessor getQueryProcessor() {
        return queryProcessor;
    }

    public ConnectionSettings setQueryProcessor(QueryProcessor queryProcessor) {
        this.queryProcessor = queryProcessor;
        return this;
    }

    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    public ConnectionSettings setRetryStrategy(RetryStrategy retryStrategy) {
        this.retryStrategy = retryStrategy;
        return this;
    }

    public RetryListener getRetryListener() {
        return retryListener;
    }

    public ConnectionSettings setRetryListener(RetryListener retryListener) {
        this.retryListener = retryListener;
        return this;
    }
}
