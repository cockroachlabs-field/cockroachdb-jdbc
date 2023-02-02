package io.cockroachdb.jdbc.it.retry;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.cockroachdb.jdbc.CockroachConnection;
import io.cockroachdb.jdbc.it.AbstractIntegrationTest;
import io.cockroachdb.jdbc.it.DatabaseFixture;
import io.cockroachdb.jdbc.it.batch.Product;
import io.cockroachdb.jdbc.it.util.JdbcTestUtils;
import io.cockroachdb.jdbc.it.util.TextUtils;
import io.cockroachdb.jdbc.retry.LoggingRetryListener;

@Order(1)
@Tag("retry-test")
@DatabaseFixture(beforeTestScript = "db/batch/product-ddl.sql")
public class ConnectionErrorRetryTest extends AbstractIntegrationTest {
    private final int NUM_PRODUCTS = 10_000;

    @Order(1)
    @Test
    public void whenStartingTest_thenInsertSomeProducts() throws SQLException {
        logger.info("Inserting {} products", NUM_PRODUCTS);

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO product (id,inventory,price,name,sku) values (?,?,?,?,?)")) {

                for (int value = 1; value <= NUM_PRODUCTS; value++) {
                    ps.setObject(1, UUID.randomUUID());
                    ps.setObject(2, 10_000);
                    ps.setObject(3, BigDecimal.TEN);
                    ps.setObject(4, "CockroachDB Unleashed 2nd Ed");
                    ps.setObject(5, UUID.randomUUID().toString());
                    ps.addBatch();
                }

                ps.executeLargeBatch();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Order(2)
    @Test
    public void whenExecutingTransactionPeriodically_thenObserveRetriesOnConnectionErrors() {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(
                Runtime.getRuntime().availableProcessors());

        Integer productCount = new JdbcTemplate(dataSource)
                .queryForObject("select count(1) from product", Integer.class);

        AtomicInteger offset = new AtomicInteger();
        AtomicInteger limit = new AtomicInteger(100);

        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();

        LoggingRetryListener retryListener = new LoggingRetryListener();

        Assertions.assertTrue(productCount > 0, "No products?");

        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(() -> {
            offset.set(offset.get() % productCount);

            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);

                if (connection.isWrapperFor(CockroachConnection.class)) {
                    connection.unwrap(CockroachConnection.class).getConnectionSettings()
                            .setRetryListener(retryListener);
                }

                List<Product> productList = new ArrayList<>();

                JdbcTestUtils.select(connection, "select * from product offset " + offset.get()
                        + " limit " + limit.get(), rs -> {
                    while (rs.next()) {
                        Product product = new Product();
                        product.setId(rs.getObject("id", UUID.class));
                        product.setInventory(rs.getInt("inventory"));
                        product.setPrice(rs.getBigDecimal("price"));
                        product.setSku(rs.getString("sku"));
                        product.setName(rs.getString("name"));

                        productList.add(product);
                    }
                });

                logger.info("Updating {} products at offset {} for at least {} - shutdown nodes / LB now at any point",
                        productList.size(),
                        offset.get(),
                        Duration.ofMillis(200).multipliedBy(productList.size())
                );

                int n = 0;
                for (Product product : productList) {
                    try {
                        System.out.printf("\n%s", TextUtils.progressBar(productList.size(), ++n));
                        Thread.sleep(200L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    int rows = JdbcTestUtils.update(connection, "update product set inventory=? where id=?",
                            product.addInventoryQuantity(-1),
                            product.getId());
                    if (rows != 1) {
                        throw new SQLException("Expected 1 got " + rows);
                    }
                }

                logger.info("Updated {} products at offset {} - committing", productList.size(), offset.get());
                connection.commit();
                commits.incrementAndGet();
            } catch (SQLException e) {
                rollbacks.incrementAndGet();
                logger.warn("", e);
            } finally {
                offset.addAndGet(limit.get());
            }
        }, 1, 20, TimeUnit.SECONDS);

        logger.info("Running for 5min");

        try {
            future.get(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            e.getCause().printStackTrace();
        } catch (TimeoutException e) {
            // fine
        } finally {
            executorService.shutdownNow();
        }

        logger.info(TextUtils.successRate("Transactions",
                commits.get(), rollbacks.get()));
        logger.info(TextUtils.successRate("Retries",
                retryListener.getSuccessfulRetries(), retryListener.getFailedRetries()));

        if (rollbacks.get() > 0) {
            logger.warn(TextUtils.flipTableVeryRoughly());
        } else {
            logger.info(TextUtils.shrug());
        }
    }
}
