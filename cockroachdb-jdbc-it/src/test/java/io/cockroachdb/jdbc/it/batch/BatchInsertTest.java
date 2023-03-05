package io.cockroachdb.jdbc.it.batch;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import io.cockroachdb.jdbc.it.AbstractIntegrationTest;
import io.cockroachdb.jdbc.it.DatabaseFixture;
import io.cockroachdb.jdbc.it.util.JdbcTestUtils;
import io.cockroachdb.jdbc.it.util.TextUtils;

@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@Order(1)
@Tag("batch-insert-test")
@DatabaseFixture(beforeTestScript = "db/batch/product-ddl.sql")
public class BatchInsertTest extends AbstractIntegrationTest {
    private static final int PRODUCTS_PER_BATCH_COUNT = 10_000;

    @Order(0)
    @ParameterizedTest
    @ValueSource(ints = {
            1 << 4, 1 << 5, 1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10})
    public void whenStartingTest_thenPrintBatches(int batchSize) throws Exception {
        logger.info("INSERT {} products using chunks of {}", PRODUCTS_PER_BATCH_COUNT, batchSize);
    }

    @Order(1)
    @ParameterizedTest
    @ValueSource(ints = {
            1 << 4, 1 << 5, 1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10})
    public void whenInsertProducts_thenObserveBulkUpdates(int batchSize) throws Exception {
        Assertions.assertFalse(TransactionSynchronizationManager.isActualTransactionActive(), "TX active");

        List<Product> products = new ArrayList<>();

        IntStream.rangeClosed(1, PRODUCTS_PER_BATCH_COUNT).forEach(value -> {
            Product product = new Product();
            product.setId(UUID.randomUUID());
            product.setInventory(1);
            product.setPrice(BigDecimal.ONE);
            product.setSku(UUID.randomUUID().toString());
            product.setName("CockroachDB Unleashed 2nd Ed");
            products.add(product);
        });

        Stream<List<Product>> chunks = JdbcTestUtils.chunkedStream(products.stream(), batchSize);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);

            logger.info("INSERT {} products using chunks of {}", PRODUCTS_PER_BATCH_COUNT, batchSize);

            final Instant startTime = Instant.now();
            final AtomicInteger n = new AtomicInteger();
            final int totalChunks = Math.round(PRODUCTS_PER_BATCH_COUNT * 1f / batchSize);

            chunks.forEach(chunk -> {
                System.out.printf("\r%s", TextUtils.progressBar(totalChunks, n.incrementAndGet(), batchSize + ""));

                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO product (id,inventory,price,name,sku) values (?,?,?,?,?)")) {

                    chunk.forEach(product -> {
                        try {
                            ps.setObject(1, product.getId());
                            ps.setObject(2, product.getInventory());
                            ps.setObject(3, product.getPrice());
                            ps.setObject(4, product.getName());
                            ps.setObject(5, product.getSku());
                            ps.addBatch();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    ps.executeLargeBatch();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            });

            logger.info("Completed in {}\n{}",
                    Duration.between(startTime, Instant.now()),
                    TextUtils.shrug());
        }
    }

    @Order(2)
    @ParameterizedTest
    @ValueSource(ints = {
            1 << 4, 1 << 5, 1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10})
    public void whenUpsertProducts_thenObserveBulkUpdates(int batchSize) throws Exception {
        Assertions.assertFalse(TransactionSynchronizationManager.isActualTransactionActive(), "TX active");

        List<Product> products = new ArrayList<>();

        IntStream.rangeClosed(1, PRODUCTS_PER_BATCH_COUNT).forEach(value -> {
            Product product = new Product();
            product.setId(UUID.randomUUID());
            product.setInventory(1);
            product.setPrice(BigDecimal.ONE);
            product.setSku(UUID.randomUUID().toString());
            product.setName("CockroachDB Unleashed 2nd Ed");
            products.add(product);
        });

        Stream<List<Product>> chunks = JdbcTestUtils.chunkedStream(products.stream(), batchSize);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);

            logger.info("UPSERT {} products using chunks of {}", PRODUCTS_PER_BATCH_COUNT, batchSize);

            final Instant startTime = Instant.now();
            final AtomicInteger n = new AtomicInteger();
            final int totalChunks = Math.round(PRODUCTS_PER_BATCH_COUNT * 1f / batchSize);

            chunks.forEach(chunk -> {
                System.out.printf("\r%s", TextUtils.progressBar(totalChunks, n.incrementAndGet(), batchSize + ""));

                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO product(id,inventory,price,name,sku)"
                                + " select"
                                + "  unnest(?) as id,"
                                + "  unnest(?) as inventory, unnest(?) as price,"
                                + "  unnest(?) as name, unnest(?) as sku"
                                + " ON CONFLICT (id) do nothing")) {
                    List<Integer> qty = new ArrayList<>();
                    List<BigDecimal> price = new ArrayList<>();
                    List<UUID> ids = new ArrayList<>();
                    List<String> name = new ArrayList<>();
                    List<String> sku = new ArrayList<>();

                    chunk.forEach(product -> {
                        ids.add(product.getId());
                        qty.add(product.getInventory());
                        price.add(product.getPrice());
                        name.add(product.getName());
                        sku.add(product.getSku());
                    });

                    ps.setArray(1, ps.getConnection().createArrayOf("UUID", ids.toArray()));
                    ps.setArray(2, ps.getConnection().createArrayOf("BIGINT", qty.toArray()));
                    ps.setArray(3, ps.getConnection().createArrayOf("DECIMAL", price.toArray()));
                    ps.setArray(4, ps.getConnection().createArrayOf("VARCHAR", name.toArray()));
                    ps.setArray(5, ps.getConnection().createArrayOf("VARCHAR", sku.toArray()));

                    ps.executeLargeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            logger.info("Completed in {}\n{}",
                    Duration.between(startTime, Instant.now()),
                    TextUtils.shrug());
        }
    }
}
