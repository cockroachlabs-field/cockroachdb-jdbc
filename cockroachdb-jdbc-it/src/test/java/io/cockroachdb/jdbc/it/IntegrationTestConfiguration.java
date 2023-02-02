package io.cockroachdb.jdbc.it;

import javax.sql.DataSource;

import org.postgresql.PGProperty;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import com.zaxxer.hikari.HikariDataSource;

import io.cockroachdb.jdbc.CockroachDataSource;
import io.cockroachdb.jdbc.CockroachProperty;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class
})
@ComponentScan(basePackageClasses = IntegrationTestConfiguration.class)
@Configuration
public class IntegrationTestConfiguration {
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @Profile("!hikari")
    public DataSource dataSource() {
        CockroachDataSource ds = dataSourceProperties()
                .initializeDataSourceBuilder()
                .type(CockroachDataSource.class)
                .build();

        ds.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");
        ds.addDataSourceProperty(PGProperty.APPLICATION_NAME.getName(), "CockroachDB JDBC Driver Test (non-pooled)");

        ds.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), "false");
        ds.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), "true");
        ds.addDataSourceProperty(CockroachProperty.RETRY_CONNECTION_ERRORS.getName(), "true");
        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "10");
        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15s");

        return loggingProxy(ds);
    }

    @Bean
    @Profile("hikari")
    @ConfigurationProperties("spring.datasource.hikari")
    public DataSource pooledDataSource() {
        HikariDataSource ds = dataSourceProperties()
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();

        ds.setMaximumPoolSize(Runtime.getRuntime().availableProcessors() * 10);
        ds.setMinimumIdle(5);
        ds.setAutoCommit(true);
        ds.setPoolName("CockroachDB JDBC HikariCP");
        ds.setConnectionTimeout(5_000); // Max wait to get connection from pool (millis)
        ds.setInitializationFailTimeout(-1); // Max wait to get connection from pool (millis)

        ds.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");
        ds.addDataSourceProperty(PGProperty.APPLICATION_NAME.getName(), "CockroachDB JDBC Driver Test (pooled)");

        ds.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), "false");
        ds.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), "true");
        ds.addDataSourceProperty(CockroachProperty.RETRY_CONNECTION_ERRORS.getName(), "true");
        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "10");
        ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "15s");

        return loggingProxy(new LazyConnectionDataSourceProxy(ds));
    }

    private ProxyDataSource loggingProxy(DataSource dataSource) {
        return ProxyDataSourceBuilder
                .create(dataSource)
                .logQueryBySlf4j(SLF4JLogLevel.TRACE, "SQL_TRACE")
                .asJson()
                .build();
    }

}
