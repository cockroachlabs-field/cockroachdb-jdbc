package io.cockroachdb.jdbc.retry;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.MDC;

import io.cockroachdb.jdbc.CockroachException;
import io.cockroachdb.jdbc.CockroachPreparedStatement;
import io.cockroachdb.jdbc.CockroachStatement;
import io.cockroachdb.jdbc.ConnectionSettings;
import io.cockroachdb.jdbc.util.Assert;
import io.cockroachdb.jdbc.util.ExceptionUtils;
import io.cockroachdb.jdbc.util.ResourceSupplier;

/**
 * A dynamic proxy interceptor / invocation handler around java.sql.Connection. This interceptor
 * provides the main retry logic for all retryable SQL exception that may surface at commit time
 * or from any associated statements or result sets.
 */
public class ConnectionRetryInterceptor extends AbstractRetryInterceptor<Connection> {
    public static Connection proxy(Connection connection,
                                   ConnectionSettings connectionSettings,
                                   ResourceSupplier<Connection> connectionSupplier) {
        return (Connection) Proxy.newProxyInstance(
                ConnectionRetryInterceptor.class.getClassLoader(),
                new Class[] {Connection.class},
                new ConnectionRetryInterceptor(connection, connectionSettings, connectionSupplier));
    }

    private final RetryListener retryListener;

    private final RetryStrategy retryStrategy;

    private final ConnectionSettings connectionSettings;

    private final ResourceSupplier<Connection> connectionSupplier;

    protected ConnectionRetryInterceptor(Connection connection,
                                         ConnectionSettings connectionSettings,
                                         ResourceSupplier<Connection> connectionSupplier) {
        super(connection);

        this.connectionSettings = connectionSettings;
        this.connectionSupplier = connectionSupplier;

        this.retryListener = connectionSettings.getRetryListener();
        this.retryStrategy = connectionSettings.getRetryStrategy();

        Assert.notNull(retryListener, "retryListener is null");
        Assert.notNull(retryStrategy, "retryStrategy");

        setMethodTraceLogger(connectionSettings.getMethodTraceLogger());
    }

    protected ConnectionSettings getConnectionSettings() {
        return connectionSettings;
    }

    @Override
    protected String connectionInfo() {
        return Objects.toString(getDelegate());
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("commit".equals(method.getName())) {
            final Instant startTime = Instant.now();

            for (int attempt = 1; ; attempt++) { // Limit defined by retry strategy
                try {
                    MethodExecution context = proceedWrapExecution(method, args);
                    if (context.hasThrowable()) {
                        throw context.getThrowable();
                    }
                    clearHistory();
                    return null;
                } catch (InvocationTargetException e) {
                    attempt = rollbackAndRetry(e.getTargetException(), method, attempt, startTime);
                }
            }
        } else if ("setAutoCommit".equals(method.getName())) {
            clearHistory();
            return proceedExecution(method, args);
        } else if ("rollback".equals(method.getName())) {
            clearHistory();
            return proceedExecution(method, args);
        } else if ("close".equals(method.getName())) {
            clearHistory();
            return proceedExecution(method, args);
        } else if ("prepareStatement".equals(method.getName())) {
            MethodExecution context = proceedWrapExecution(method, args);
            if (context.hasThrowable()) {
                throw context.getTargetException();
            }
            addMethodExecution(context);

            CockroachPreparedStatement cockroachPreparedStatement
                    = new CockroachPreparedStatement((PreparedStatement) context.getResult());
            PreparedStatement preparedStatementRetryProxy
                    = PreparedStatementRetryInterceptor.proxy(cockroachPreparedStatement, this);
            context.setResult(preparedStatementRetryProxy);
            return preparedStatementRetryProxy;
        } else if ("createStatement".equals(method.getName())) {
            MethodExecution context = proceedWrapExecution(method, args);
            if (context.hasThrowable()) {
                throw context.getTargetException();
            }
            addMethodExecution(context);

            CockroachStatement cockroachStatement
                    = new CockroachStatement((Statement) context.getResult(),
                    Optional.ofNullable(connectionSettings.getQueryProcessor()).orElse((connection, sql) -> sql));
            Statement statementRetryProxy
                    = StatementRetryInterceptor.proxy(cockroachStatement, this);
            context.setResult(statementRetryProxy);
            return statementRetryProxy;
        } else if ("toString".equals(method.getName())
                || "isWrapperFor".equals(method.getName())
                || "unwrap".equals(method.getName())
                || "hashCode".equals(method.getName())) {
            return proceed(method, args);
        }

        MethodExecution context = proceedWrapExecution(method, args);
        if (context.hasThrowable()) {
            throw context.getTargetException();
        }
        addMethodExecution(context);
        return context.getResult();
    }

    protected final int rollbackAndRetry(Throwable targetException, Method method, int attempt, Instant startTime)
            throws Throwable {
        Assert.isTrue(attempt > 0, "attempt must be > 0");

        if (!(targetException instanceof SQLException)) {
            throw targetException;
        }

        final SQLException rootCauseException = (SQLException) targetException;

        if (!retryStrategy.isRetryableException(rootCauseException)) {
            throw rootCauseException;
        }

        logger.debug("Entering retry attempt [{}] due to transient SQL exception:\n{}",
                attempt, ExceptionUtils.toNestedString(rootCauseException));

        for (; ; attempt++) {
            final int currentAttempt = attempt;

            if (!retryStrategy.proceedWithRetry(attempt, startTime)) {
                throw new SurrenderRetryException("Too many retry attempts [" + attempt
                        + "] or other limit in [" + retryStrategy.getDescription() + "]", rootCauseException);
            }

            MDC.put("retry.connection", connectionInfo());
            MDC.put("retry.attempt", attempt + "");

            retryListener.beforeRetry(method.toGenericString(), attempt, rootCauseException,
                    Duration.between(startTime, Instant.now()));

            closeDelegate(attempt);

            // Pause current thread for a delay determined by strategy
            retryStrategy.waitBeforeRetry(attempt, startTime, duration ->
                    logger.debug("Waiting [{}] for attempt [{}]", duration, currentAttempt));

            if (openDelegate(attempt, rootCauseException)) {
                SQLException retryException = rootCauseException;
                try {
                    // At this point we have a new, valid connection delegate and ready to replay history
                    retry(getDelegate());
                    break;
                } catch (CockroachException ex) {
                    retryException = ex;
                    throw ex.initCause(rootCauseException);
                } catch (SQLException ex) {
                    retryException = ex;
                    if (!retryStrategy.isRetryableException(ex)) {
                        throw ex.initCause(rootCauseException);
                    }
                } finally {
                    retryListener.afterRetry(method.toGenericString(), attempt, retryException,
                            Duration.between(startTime, Instant.now()));
                    MDC.clear();
                }
            } else {
                MDC.clear();
            }
        }

        return attempt;
    }

    private boolean openDelegate(int attempt, SQLException sqlException) throws SQLException {
        try {
            logger.debug("Opening new connection for attempt [{}]", attempt);
            Connection delegate = connectionSupplier.get();
            if (delegate.getAutoCommit()) {
                throw new UncategorizedRetryException("Connection cannot be in auto-commit mode", sqlException);
            }
            if (!delegate.isValid(10)) {
                throw new UncategorizedRetryException("Connection is invalid", sqlException);
            }
            Connection expiredDelegate = getDelegate();
            setDelegate(delegate);
            logger.debug("Opened new connection delegate [{}] replacing [{}]", delegate, expiredDelegate);
        } catch (SQLException ex) {
            // Unless it's a retryable error (which may include connection error) we can't continue
            if (!retryStrategy.isRetryableException(ex)) {
                throw ex;
            }
            logger.warn("SQL exception in connection attempt [{}]\n{}",
                    attempt, ExceptionUtils.toNestedString(ex));
            return false;
        }
        return true;
    }

    private void closeDelegate(int attempt) throws SQLException {
        Connection expiredDelegate = getDelegate();
        if (expiredDelegate.isClosed()) {
            logger.debug("Connection delegate [{}] already closed for attempt [{}]",
                    expiredDelegate, attempt);
            return;
        }
        try {
            logger.debug("Rollback and close connection delegate [{}] for attempt [{}]",
                    expiredDelegate, attempt);
            SQLWarning warning = expiredDelegate.getWarnings();
            if (warning != null) {
                logger.debug("There are warnings:\n{}", ExceptionUtils.toNestedString(warning));
            }
            expiredDelegate.rollback();
            expiredDelegate.close();
        } catch (SQLException ex) {
            // Unless it's a connection related error, we can't continue
            if (!retryStrategy.isConnectionError(ex)) {
                throw new UncategorizedRetryException("Exception on rollback", ex);
            }
            // Let connection errors pass through with a warning since these are potentially retried
            logger.warn("SQL exception in rollback for connection delegate [{}]\n{}",
                    expiredDelegate, ExceptionUtils.toNestedString(ex));
        }
    }

    @Override
    protected void doRetry(Iterable<MethodExecution> methodExecutions)
            throws Throwable {
        for (MethodExecution methodExecution : methodExecutions) {
            MethodExecution context = proceedWrapExecution(methodExecution.getMethod(),
                    methodExecution.getMethodArgs());
            if (context.hasThrowable()) {
                throw context.getTargetException();
            }

            Object firstResult = methodExecution.getResult();
            Object lastResult = context.getResult();

            if (firstResult != null && Proxy.isProxyClass(firstResult.getClass())) {
                if (Proxy.getInvocationHandler(firstResult) instanceof PreparedStatementRetryInterceptor) {
                    PreparedStatementRetryInterceptor firstProxy =
                            (PreparedStatementRetryInterceptor) Proxy.getInvocationHandler(firstResult);
                    firstProxy.retry((PreparedStatement) lastResult);
                } else if (Proxy.getInvocationHandler(firstResult) instanceof StatementRetryInterceptor) {
                    StatementRetryInterceptor firstProxy =
                            (StatementRetryInterceptor) Proxy.getInvocationHandler(firstResult);
                    firstProxy.retry((Statement) lastResult);
                } else {
                    throw new UncategorizedRetryException("Unknown JDBC proxy: " + firstResult);
                }
            }
        }
    }
}
