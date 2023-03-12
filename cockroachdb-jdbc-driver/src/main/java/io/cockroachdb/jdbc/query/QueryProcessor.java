package io.cockroachdb.jdbc.query;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface representing a SQL query processor for rewriting queries.
 */
public interface QueryProcessor {
    /**
     * A no-op processor.
     */
    QueryProcessor PASS_THROUGH = new QueryProcessor() {
        @Override
        public String processQuery(Connection connection, String query) {
            return query;
        }

        @Override
        public boolean isTransactionScoped() {
            return false;
        }
    };

    String processQuery(Connection connection, String query) throws SQLException;

    boolean isTransactionScoped();
}
