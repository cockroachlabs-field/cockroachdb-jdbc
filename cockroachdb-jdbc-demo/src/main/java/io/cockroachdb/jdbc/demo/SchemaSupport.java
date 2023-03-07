package io.cockroachdb.jdbc.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Statement;

import javax.sql.DataSource;

public abstract class SchemaSupport {
    private SchemaSupport() {
    }

    public static void setupSchema(DataSource ds) throws Exception {
        InputStream is = SimpleJdbcDemo.class.getResourceAsStream("/db/create.sql");
        if (is == null) {
            throw new IOException("Not found: " + "/db/create.sql");
        }
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

        StringBuilder buffer = new StringBuilder();
        String line = reader.readLine();
        while (line != null) {
            if (!line.startsWith("--") && !line.isEmpty()) {
                buffer.append(line);
            }
            if (line.endsWith(";") && buffer.length() > 0) {
                JdbcTemplate.execute(ds, conn -> {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(buffer.toString());
                    }
                    buffer.setLength(0);
                    return null;
                });
            }
            line = reader.readLine();
        }
    }
}
