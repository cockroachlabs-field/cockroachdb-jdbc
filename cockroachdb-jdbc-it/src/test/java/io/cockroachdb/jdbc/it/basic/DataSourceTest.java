package io.cockroachdb.jdbc.it.basic;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.cockroachdb.jdbc.CockroachDriver;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.proxy.JdbcProxyFactory;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

@Disabled
public class DataSourceTest {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceTest.class);

    @Test
    public void whenUsingDataSource_expectConnection() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(CockroachDriver.class.getName());
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(5);
        config.setInitializationFailTimeout(60000);
        config.setAllowPoolSuspension(true);
        config.setKeepaliveTime(60000);
        config.setMaxLifetime(1800000);
        config.setConnectionTimeout(10000);
        config.setConnectionInitSql("select 1");

        ConnectionDetails details = ConnectionDetails.getInstance();
        config.setJdbcUrl(details.getUrl());
        config.setUsername(details.getUser());
        config.setPassword(details.getPassword());

        config.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), true);

        DataSource dataSource = ProxyDataSourceBuilder
                .create(new HikariDataSource(config))
                .logQueryBySlf4j(SLF4JLogLevel.TRACE, logger.getName())
                .asJson()
                .multiline()
                .jdbcProxyFactory(JdbcProxyFactory.DEFAULT)
                .build();

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("select version()")) {
            while (rs.next()) {
                logger.info("Connected to [{}]", rs.getString(1));
            }

            listMetadata(connection.getMetaData());
        }
    }

    private static void listMetadata(DatabaseMetaData metaData) throws SQLException {
        Map<String, Object> tuples = new LinkedHashMap<>();

        tuples.put("databaseProductName", metaData.getDatabaseProductName());
        tuples.put("databaseMajorVersion", metaData.getDatabaseMajorVersion());
        tuples.put("databaseMinorVersion", metaData.getDatabaseMinorVersion());
        tuples.put("databaseProductVersion", metaData.getDatabaseProductVersion());
        tuples.put("driverMajorVersion", metaData.getDriverMajorVersion());
        tuples.put("driverMinorVersion", metaData.getDriverMinorVersion());
        tuples.put("driverName", metaData.getDriverName());
        tuples.put("driverVersion", metaData.getDriverVersion());
        tuples.put("maxConnections", metaData.getMaxConnections());
        tuples.put("defaultTransactionIsolation", metaData.getDefaultTransactionIsolation());

        tuples.forEach((k, v) -> logger.info("{}: {} ({})", k, v, v.getClass().getName()));
    }
}
