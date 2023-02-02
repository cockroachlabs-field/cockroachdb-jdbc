package io.cockroachdb.jdbc.it.basic;

public class ConnectionDetails {
    public static final ConnectionDetails getInstance() {
        return new ConnectionDetails();
    }

    private final String url = "jdbc:cockroachdb://localhost:26257/jdbc_test?sslmode=disable";

    private final String user = "root";

    private final String password = "";

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
