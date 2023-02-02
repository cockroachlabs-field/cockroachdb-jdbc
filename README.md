# CockroachDB JDBC Driver

[![Java CI](https://github.com/cockroachlabs-field/cockroachdb-jdbc/actions/workflows/maven-publish.yml/badge.svg?branch=main)](https://github.com/cockroachdb/cockroachdb-jdbc/actions/workflows/maven-publish.yml)
[![coverage](.github/badges/jacoco.svg)](https://github.com/cockroachlabs-field/cockroachdb-jdbc/actions/workflows/maven-publish.yml)
[![branches coverage](.github/badges/branches.svg)](https://github.com/cockroachlabs-field/cockroachdb-jdbc/actions/workflows/maven-publish.yml)

<img align="left" src="docs/logo.png" />

An open-source JDBC Type-4 driver for [CockroachDB](https://www.cockroachlabs.com/) that wraps the PostgreSQL 
JDBC driver ([pgjdbc](https://jdbc.postgresql.org/)) and communicates in the PostgreSQL native network 
wire (v3.0) protocol with CockroachDB.   

## Disclaimer

This driver is an experimental prototype and not officially supported by Cockroach Labs. Use of this driver
is entirely at your own risk and Cockroach Labs makes no guarantees or warranties about its operation.

See [MIT](LICENSE.txt) license for terms and conditions.

## Features

This JDBC driver adds some features on top of pgJDBC that are relevant to CockroachDB. Such as:

- Internal retries on serialization conflicts.
- Internal retries on connection errors.
- Rewriting qualified SQL queries to use [SELECT FOR UPDATE](https://www.cockroachlabs.com/docs/stable/select-for-update.html) 
to reduce retries on serialization conflicts. 
- CockroachDB specific database metadata.

All these features are disabled by default which means the driver is mainly operating in a pass-through mode 
delegating all JDBC API invocations to the pgJDBC driver. 

Enabling retries for example may reduce the need for application-level retry logic and enhance compatibility 
with 3rd-party products that don't implement any transaction retries. Also enabling `SELECT FOR UPDATE` rewrites 
may reduce the need for retries to a bare minimum, at the expense of imposing locks on every read operation. 

See the [design notes](docs/DESIGN.md) of how driver-level retries works and its limitations.

For more information about client-side retry logic, see also:

- [Transaction Contention](https://www.cockroachlabs.com/docs/stable/performance-best-practices-overview.html#transaction-contention)
- [Connection Retry Loop](https://www.cockroachlabs.com/docs/stable/node-shutdown.html#connection-retry-loop)

## Getting Started

Example of creating a JDBC connection and executing a simple query:

```java
try (Connection connection 
        = DriverManager.getConnection("jdbc:cockroachdb://localhost:26257/jdbc_test?sslmode=disable") {
  try (Statement statement = connection.createStatement()) {
    try (ResultSet rs = statement.executeQuery("select version()")) {
      if (rs.next()) {
        System.out.println(rs.getString(1));
      }
    }
  }
}
```

## Maven configuration

Add this dependency to your `pom.xml` file:

```xml
<dependency>
    <groupId>io.cockroachdb.jdbc</groupId>
    <artifactId>cockroachdb-jdbc-driver</artifactId>
    <version>{version}</version>
</dependency>
```

Alternatively, if you just need to drop in the driver JAR file to your projects `lib` directory, or similar:

```bash
git clone git@github.com:cockroachlabs-field/cockroachdb-jdbc.git
cd cockroachdb-jdbc
chmod +x mvnw
./mvnw clean install
```

The JDBC driver jar is now found in `cockroachdb-jdbc-driver/target`.

## Modules

### cockroachdb-jdbc-driver

The main library for the CockroachDB JDBC driver.

### cockroachdb-jdbc-it

Integration and functional tests activated via a Maven profile. See build section further down in this page.

## Getting Help

### Reporting Issues

Spring Data CockroachDB uses [GitHub](https://github.com/cockroachlabs-field/cockroachdb-jdbc/issues) as issue tracking system
to record bugs and feature requests. If you want to raise an issue, please follow the recommendations below:

* Before you log a bug, please search the [issue tracker](https://github.com/cockroachlabs-field/cockroachdb-jdbc/issues)
  to see if someone has already reported the problem.
* If the issue doesn't exist already, [create a new issue](https://github.com/cockroachlabs-field/cockroachdb-jdbc/issues).
* Please provide as much information as possible with the issue report, we like to know the version of Spring Data
  that you are using and JVM version, complete stack traces and any relevant configuration information.
* If you need to paste code, or include a stack trace format it as code using triple backtick.

### Supported CockroachDB and JRE Versions

This driver is CockroachDB version agnostic and supports any version supported by the PostgreSQL 
JDBC driver v 42.5+ (pgwire protocol v3.0). 

It's build for Java 8 (or above) at language source and target level.

## URL Properties

This driver uses the `jdbc:cockroachdb:` JDBC URL prefix and supports all PostgreSQL URL properties 
on top of that. To configure a datasource to use this driver, you typically configure it for PostgreSQL
and only change the URL prefix and the driver class name.

The general format for a JDBC URL for connecting to a CockroachDB server:

    jdbc:cockroachdb:[//host[:port]/][database][?property1=value1[&property2=value2]...]

See [pgjdbc](https://github.com/pgjdbc/pgjdbc) for all supported driver properties
and the semantics.

In addition, this driver has the following CockroachDB specific properties:
                         
### retryTransientErrors 

(default: false) 

The JDBC driver will automatically retry serialization failures 
(40001 [state code](https://github.com/pgjdbc/pgjdbc/blob/master/pgjdbc/src/main/java/org/postgresql/util/PSQLState.java)) 
at read, write or commit time. This is done by keeping track of all statements and the results during a transaction, 
and if the transaction is aborted due to a transient 40001 error, it will rollback and retry the recorded operations 
on a new connection and compare the results with the initial commit attempt. If the results are different, the
driver will be forced to give up the retry attempt to preserve a serializable outcome. 

Enable this option if you want to handle aborted transactions internally in the driver, preferably combined with 
select-for-update locking. Leave this option disabled if you want to handle aborted transactions in your 
own application.

### retryConnectionErrors 

(default: false) 

The CockroachDB JDBC driver will automatically retry transient connection errors with SQL state 
08001, 08003, 08004, 08006, 08007, 08S01 or 57P01 at read, write or commit time.

Applicable only when `retryTransientErrors` is also true.

Disable this option if you want to handle connection errors in your own application or connection pool.

**CAUTION!** Retrying on non-serializable conflict errors (i.e anything but 40001) may produce duplicate outcomes
if the SQL statements are non-idempotent. See the [design notes](docs/DESIGN.md) for more details..

### retryListenerClassName 

(default: `io.cockroachdb.jdbc.retry.EmptyRetryListener`)

Name of class that implements 'io.cockroachdb.jdbc.retry.RetryListener' to be used to receive
callback events when retries occur. One instance is created for each JDBC connection.

### retryStrategyClassName 

(default: `io.cockroachdb.jdbc.retry.ExponentialBackoffRetryStrategy`)

Name of class that implements `io.cockroachdb.jdbc.retry.RetryStrategy` to be used when `retryTransientErrors` 
property is set to `true`. If this class also implements `io.cockroachdb.jdbc.proxy.RetryListener` it will receive 
callback events when retries happen. One instance of this class is created for each JDBC connection.

The default `ExponentialBackoffRetryStrategy` will use an exponentially increasing delay
with jitter and a multiplier of 2 up to the limit set by `retryMaxBackoffTime`. 

### retryMaxAttempts 

(default: 15) 

Maximum number of retry attempts on transient failures (connection errors / serialization conflicts). 
If this limit is exceeded, the driver will throw a SQL exception with the same state code signalling 
its yielding further retry attempts.

### retryMaxBackoffTime 

(default: 30s) 

Maximum exponential backoff time in format of a duration expression (like `12s`).
The duration applies for the total time for all retry attempts at transaction level.

Applicable only when `retryTransientErrors` is true.

### implicitSelectForUpdate 

(default: false) 

The driver will automatically append a `FOR UPDATE` clause to all qualified `SELECT` statements.
The qualifying requirements include:

- Not used in a read-only connection
- No time travel clause (`as of system time`) 
- No aggregate functions
- No group by or distinct operators 
- Not referencing internal table schema

A `SELECT .. FOR UPDATE` will lock the rows returned by a selection query such that other transactions 
trying to access those rows are forced to wait for the transaction that locked the rows to finish. 
These other transactions are effectively put into a queue based on when they tried to read the value 
of the locked rows. It does not eliminate the chance of serialization conflicts but greatly reduces it.

### useCockroachMetadata 

(default: false) 

By default, the driver will use PostgreSQL JDBC driver metadata provided in `java.sql.DatabaseMetaData` 
rather than CockroachDB specific metadata. While the latter is more correct, it causes incompatibilities 
with libraries that bind to PostgreSQL version details, such as Flyway and other tools.

## Logging

This driver uses [SLF4J](https://www.slf4j.org/) for logging which means its agnostic to the logging 
framework used by the application. The JDBC driver module does not include any logging framework 
dependency transitively.

## Examples

### Plain Java Example

```java
Class.forName(CockroachDriver.class.getName());

try (Connection connection 
        = DriverManager.getConnection("jdbc:cockroachdb://localhost:26257/jdbc_test?sslmode=disable&implicitSelectForUpdate=true&retryTransientErrors=true") {
  try (Statement statement = connection.createStatement()) {
    try (ResultSet rs = statement.executeQuery("select version()")) {
      if (rs.next()) {
        System.out.println(rs.getString(1));
      }
    }
  }
}
```

### Spring Boot Example

Configure the datasource in `src/main/resources/application.yml`:

```yml
spring:
  datasource:
    driver-class-name: io.cockroachdb.jdbc.CockroachDriver
    url: "jdbc:cockroachdb://localhost:26257/jdbc_test?sslmode=disable&application_name=MyTestAppe&implicitSelectForUpdate=true&retryTransientErrors=true"
    username: root
    password:
```

Optionally, configure the datasource programmatically and use the 
[TTDDYY](https://github.com/jdbc-observations/datasource-proxy) datasource logging proxy:

```java
@Bean
@Primary
public DataSource dataSource() {
    return ProxyDataSourceBuilder
            .create(hikariDataSource())
            .traceMethods()
            .logQueryBySlf4j(SLF4JLogLevel.TRACE, "SQL_TRACE")
            .asJson()
            .multiline()
            .build();
}

@Bean
@ConfigurationProperties("spring.datasource.hikari")
public HikariDataSource hikariDataSource() {
    HikariDataSource ds = dataSourceProperties()
            .initializeDataSourceBuilder()
            .type(HikariDataSource.class)
            .build();
    ds.setAutoCommit(false);
    ds.addDataSourceProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");
    
    ds.addDataSourceProperty(CockroachProperty.IMPLICIT_SELECT_FOR_UPDATE.getName(), "true");
    ds.addDataSourceProperty(CockroachProperty.RETRY_TRANSIENT_ERRORS.getName(), "true");
    ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_ATTEMPTS.getName(), "5");
    ds.addDataSourceProperty(CockroachProperty.RETRY_MAX_BACKOFF_TIME.getName(), "10000");
    
    return ds;
}
```

To configure `src/main/resources/logback-spring.xml` to capture all SQL statements and JDBC API calls:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <include resource="org/springframework/boot/logging/logback/defaults.xml"/>
    <include resource="org/springframework/boot/logging/logback/console-appender.xml" />

    <logger name="org.springframework" level="INFO"/>
    
    <logger name="io.cockroachdb.jdbc" level="DEBUG"/>
    <logger name="SQL_TRACE" level="TRACE"/>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

## Versioning

This library follows [Semantic Versioning](http://semver.org/).

## Building

The CockroachDB JDBC driver runs on any platform for which there is a JRE (8+).

### Prerequisites

- JDK8+ with 1.8 language level (OpenJDK compatible)
- Maven 3+ (optional, embedded)

If you want to build with the regular mvn command, you will need [Maven v3.5.0](https://maven.apache.org/run-maven/index.html) or above.

Install the JDK (Linux):

```bash
sudo apt-get -qq install -y openjdk-8-jdk
```

### Clone the project

```bash
git clone git@github.com:cockroachlabs-field/cockroachdb-jdbc.git
cd cockroachdb-jdbc
```

### Build the project

```bash
chmod +x mvnw
./mvnw clean install
```

The JDBC driver jar is now found in `cockroachdb-jdbc-driver/target`.
                  
### Run Integration Tests
     
The integration tests will run through a series of contended workloads to exercise the 
retry mechanism and other driver features.

First start a [local](https://www.cockroachlabs.com/docs/stable/start-a-local-cluster.html) CockroachDB node or cluster.

Create the database:

```bash
cockroach sql --insecure --host=localhost -e "CREATE database jdbc_test"
```

Then activate the integration test Maven profile:

```bash
./mvnw -P it -Dgroups=anomaly-test clean install
```

See the [pom.xml](pom.xml) file for changing the database URL and other settings (under `ìt` profile).

## Terms of Use

See [MIT](LICENSE.txt) for terms and conditions.
