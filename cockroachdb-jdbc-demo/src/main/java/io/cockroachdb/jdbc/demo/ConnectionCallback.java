package io.cockroachdb.jdbc.demo;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
interface ConnectionCallback<T> {
    T doInConnection(Connection conn) throws SQLException;
}
