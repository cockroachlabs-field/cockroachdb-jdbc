package io.cockroachdb.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.jdbc.query.SelectForUpdateProcessor;
import io.cockroachdb.jdbc.retry.ConnectionRetryInterceptor;
import io.cockroachdb.jdbc.retry.MethodTraceLogger;
import io.cockroachdb.jdbc.retry.RetryListener;
import io.cockroachdb.jdbc.retry.RetryStrategy;

/**
 * A {@code java.sql.Driver} implementation for CockroachDB, wrapping an underlying
 * {@code org.postgresql.Driver} delegate.
 */
public class CockroachDriver implements Driver {
    public static final String DRIVER_PREFIX = "jdbc:cockroachdb";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static CockroachDriver singletonInstance;

    static {
        try {
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static boolean isRegistered() {
        return singletonInstance != null;
    }

    public static void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException("CockroachDBDriver is already registered!");
        }
        CockroachDriver registeredDriver = new CockroachDriver();
        DriverManager.registerDriver(registeredDriver);
        CockroachDriver.singletonInstance = registeredDriver;
    }

    public static void unregister() throws SQLException {
        if (singletonInstance == null) {
            throw new IllegalStateException(
                    "CockroachDBDriver is not registered or it has not been registered using Driver.register() method");
        }
        DriverManager.deregisterDriver(singletonInstance);
        singletonInstance = null;
    }

    public static CockroachDriver getRegisteredDriver() throws SQLException {
        if (isRegistered()) {
            return singletonInstance;
        }
        throw new SQLException("The Cockroach driver has not been registered!");
    }

    public static String toDelegateURL(String url) {
        return url.replace(DRIVER_PREFIX, "jdbc:postgresql");
    }

    //////////////////////////////////////////////////////////////////////

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("url is null");
        }

        if (!url.startsWith(DRIVER_PREFIX)) {
            return null;
        }

        logger.info("Opening connection to \"{}\" using {} with properties {}",
                url, CockroachDriverInfo.DRIVER_FULL_NAME, info);

        Properties defaults = new Properties();
        defaults.putAll(info);

        Properties properties = org.postgresql.Driver.parseURL(toDelegateURL(url), defaults);
        if (properties == null) {
            throw new CockroachException("Error parsing JDBC URL", PSQLState.UNEXPECTED_ERROR);
        }

        Connection psqlConnection = DriverManager.getConnection(toDelegateURL(url), info);

        ConnectionSettings connectionSettings = new ConnectionSettings();
        connectionSettings.setUseCockroachMetadata(Boolean.parseBoolean(
                CockroachProperty.USE_COCKROACH_METADATA.toDriverPropertyInfo(properties).value));

        if (Boolean.parseBoolean(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.toDriverPropertyInfo(properties).value)) {
            connectionSettings.setQueryProcessor(new SelectForUpdateProcessor());
        }

        if (Boolean.parseBoolean(CockroachProperty.RETRY_TRANSIENT_ERRORS.toDriverPropertyInfo(properties).value)) {
            connectionSettings.setRetryStrategy(loadRetryStrategy(properties));
            connectionSettings.setRetryListener(loadRetryListener(properties));

            if (logger.isTraceEnabled()) {
                connectionSettings.setMethodTraceLogger(MethodTraceLogger.createInstance(logger).setMasked(false));
            }

            CockroachConnection cockroachConnection = new CockroachConnection(psqlConnection, connectionSettings);

            return ConnectionRetryInterceptor.proxy(cockroachConnection, connectionSettings,
                    () -> {
                        Connection connection = DriverManager.getConnection(toDelegateURL(url), info);
                        connection.setAutoCommit(false);
                        return new CockroachConnection(connection, connectionSettings);
                    });
        } else {
            if (Boolean.parseBoolean(
                    CockroachProperty.RETRY_CONNECTION_ERRORS.toDriverPropertyInfo(properties).value)) {
                logger.warn("JDBC driver property \"{}\" requires also \"{}\" set be to true to take effect",
                        CockroachProperty.RETRY_CONNECTION_ERRORS.getName(),
                        CockroachProperty.RETRY_TRANSIENT_ERRORS.getName());
            }

            return new CockroachConnection(psqlConnection, connectionSettings);
        }
    }

    @SuppressWarnings("unchecked")
    protected RetryStrategy loadRetryStrategy(Properties properties) throws SQLException {
        String className = CockroachProperty.RETRY_STRATEGY_CLASSNAME.toDriverPropertyInfo(properties).value;
        try {
            Class<RetryStrategy> retryStrategyClass = (Class<RetryStrategy>) Class.forName(className);
            RetryStrategy retryStrategy = retryStrategyClass.getDeclaredConstructor().newInstance();
            retryStrategy.configure(properties);
            return retryStrategy;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new CockroachException("Unable to create instance of retry strategy: " + className,
                    PSQLState.UNEXPECTED_ERROR, e);
        }
    }

    @SuppressWarnings("unchecked")
    protected RetryListener loadRetryListener(Properties properties) throws SQLException {
        String className = CockroachProperty.RETRY_LISTENER_CLASSNAME.toDriverPropertyInfo(properties).value;
        try {
            Class<RetryListener> retryListenerClass = (Class<RetryListener>) Class.forName(className);
            RetryListener retryListener = retryListenerClass.getDeclaredConstructor().newInstance();
            retryListener.configure(properties);
            return retryListener;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new CockroachException("Unable to create instance of retry listener: " + className,
                    PSQLState.UNEXPECTED_ERROR, e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        if (!url.startsWith(DRIVER_PREFIX)) {
            logger.warn("JDBC URL must start with \"{}\" but was: {}", DRIVER_PREFIX, url);
            return false;
        }
        return true;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        org.postgresql.Driver delegate = new org.postgresql.Driver();

        List<DriverPropertyInfo> infos
                = new ArrayList<>(Arrays.asList(delegate.getPropertyInfo(toDelegateURL(url), info)));

        Properties infoCopy = new Properties(info);
        Properties parsed = org.postgresql.Driver.parseURL(toDelegateURL(url), infoCopy);
        if (parsed != null) {
            Arrays.stream(CockroachProperty.values()).forEach(cockroachProperty
                    -> infos.add(cockroachProperty.toDriverPropertyInfo(parsed)));
        } else {
            Arrays.stream(CockroachProperty.values()).forEach(cockroachProperty
                    -> infos.add(cockroachProperty.toDriverPropertyInfo(infoCopy)));
        }

        return infos.toArray(new DriverPropertyInfo[] {});
    }

    @Override
    public int getMajorVersion() {
        return CockroachDriverInfo.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return CockroachDriverInfo.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // same as pg-jdbc
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return PARENT_LOGGER;
    }

    private static final java.util.logging.Logger PARENT_LOGGER = java.util.logging.Logger.getLogger(
            "io.cockroachdb.jdbc");
}
