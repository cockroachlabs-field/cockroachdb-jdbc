package io.cockroachdb.jdbc.it.util.util;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ResultSetHandler {
    void process(ResultSet rs) throws SQLException;
}
