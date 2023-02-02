package io.cockroachdb.jdbc.it.basic;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cockroachdb.jdbc.CockroachDataSource;
import io.cockroachdb.jdbc.CockroachDriver;
import io.cockroachdb.jdbc.CockroachDriverInfo;
import io.cockroachdb.jdbc.CockroachProperty;

@Disabled
public class DriverManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(DriverManagerTest.class);

    @Test
    public void whenUsingDataSource_expectValidConnection() throws Exception {
        Class.forName(CockroachDriver.class.getName());

        ConnectionDetails details = ConnectionDetails.getInstance();
        CockroachDataSource ds = new CockroachDataSource();
        ds.setUrl(details.getUrl());
        ds.setUsername(details.getUser());
        ds.setPassword(details.getPassword());
        ds.addDataSourceProperty(CockroachProperty.USE_COCKROACH_METADATA.getName(), "true");

        try (Connection connection = ds.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "select version()")) {
            while (rs.next()) {
                logger.info("Connected to [{}]", rs.getString(1));
            }

            listMetadata(connection.getMetaData());
        }
    }

    @Test
    public void whenUsingDriverManager_expectValidConnection() throws Exception {
        Class.forName(CockroachDriver.class.getName());

        ConnectionDetails details = ConnectionDetails.getInstance();

        try (Connection connection = DriverManager.getConnection(
                details.getUrl() + "&useCockroachMetadata=true", details.getUser(), details.getPassword());
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "select version()")) {
            while (rs.next()) {
                logger.info("Connected to [{}]", rs.getString(1));
            }

            listMetadata(connection.getMetaData());
        }
    }

    private static void listMetadata(DatabaseMetaData metaData) throws SQLException {
        Map<String, Object> tuples = new LinkedHashMap<>();

        Assertions.assertEquals("CockroachDB", metaData.getDatabaseProductName());
        Assertions.assertEquals("CockroachDB JDBC Driver", metaData.getDriverName());
        Assertions.assertEquals(CockroachDriverInfo.DRIVER_VERSION, metaData.getDriverVersion());
        Assertions.assertEquals(CockroachDriverInfo.MAJOR_VERSION, metaData.getDriverMajorVersion());
        Assertions.assertEquals(CockroachDriverInfo.MINOR_VERSION, metaData.getDriverMinorVersion());
        Assertions.assertEquals(8, metaData.getDefaultTransactionIsolation());
        Assertions.assertFalse(metaData.supportsStoredProcedures());

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
