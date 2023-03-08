package io.cockroachdb.jdbc.retry;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.cockroachdb.jdbc.util.Assert;

public abstract class AbstractRetryInterceptor<T> extends AbstractInterceptor<T> {
    private MethodTraceLogger methodTraceLogger;

    private final List<MethodExecution> methodExecutions = new ArrayList<>();

    public AbstractRetryInterceptor(T delegate) {
        super(delegate);
    }

    public void setMethodTraceLogger(MethodTraceLogger methodTraceLogger) {
        this.methodTraceLogger = methodTraceLogger;
    }

    protected void addMethodExecution(MethodExecution methodExecution) {
        Assert.notNull(methodExecution, "methodExecution is null");
        methodExecutions.add(methodExecution);
    }

    protected void clearHistory() {
        methodExecutions.clear();
    }

    protected MethodExecution proceedWrapExecution(Method method, Object[] args) {
        Assert.notNull(method, "method is null");

        Instant callTime = Instant.now();
        Duration executionTime;
        Throwable ex = null;
        Object result = null;

        try {
            result = proceed(method, args);
        } catch (Throwable e) {
            ex = e;
        } finally {
            executionTime = Duration.between(callTime, Instant.now());
            if (methodTraceLogger != null) {
                methodTraceLogger.log(ex, executionTime, connectionInfo(), getDelegate(), method, args);
            }
        }

        return MethodExecution.builder()
                .withTarget(getDelegate())
                .withMethod(method)
                .withMethodArgs(args)
                .withResult(result)
                .withThrowable(ex)
                .withExecutionTime(executionTime)
                .withConnectionInfo(connectionInfo())
                .build();
    }

    protected Object proceedExecution(Method method, Object[] args) throws Throwable {
        Assert.notNull(method, "method is null");

        Instant callTime = Instant.now();
        Throwable ex = null;

        try {
            return proceed(method, args);
        } catch (Throwable e) {
            ex = e;
            throw e;
        } finally {
            if (methodTraceLogger != null) {
                methodTraceLogger.log(ex, Duration.between(callTime, Instant.now()), connectionInfo(),
                        getDelegate(), method, args);
            }
        }
    }

    protected final void retry(T delegate) throws Throwable {
        setDelegate(delegate);
        if (logger.isDebugEnabled()) {
            logMethodExecutions();
        }
        doRetry(Collections.unmodifiableList(methodExecutions));
    }

    private void logMethodExecutions() {
        StringBuilder results = new StringBuilder();
        int methodCount = 0;

        for (MethodExecution methodExecution : methodExecutions) {
            results.append("\n\t[");
            results.append(methodCount);
            results.append("] ");
            results.append(methodExecution.getMethod().toGenericString());
            methodCount++;
            if (methodCount > 30) {
                results.append("\n\t(truncated at 30 but there are ")
                        .append(methodExecutions.size()).append(" methods in total)");
                break;
            }
        }

        logger.debug("Repeating [{}] method executions for delegate [{}]: {}",
                methodCount, getDelegate().toString(), results);
    }

    protected abstract String connectionInfo();

    protected abstract void doRetry(Iterable<MethodExecution> methodExecutions)
            throws Throwable;
}
