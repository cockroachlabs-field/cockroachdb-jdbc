package io.cockroachdb.jdbc.query;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A functional interface representing a SQL query processor for rewriting queries.
 */
@FunctionalInterface
public interface QueryProcessor {
    String processQuery(Connection connection, String query) throws SQLException;
}
