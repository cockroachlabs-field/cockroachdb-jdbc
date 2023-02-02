package io.cockroachdb.jdbc.it.anomaly;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import io.cockroachdb.jdbc.it.AbstractIntegrationTest;
import io.cockroachdb.jdbc.it.util.BoundedThreadPool;

@Tag("anomaly-test")
public abstract class AbstractAnomalyTest extends AbstractIntegrationTest {
    protected static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

    protected static final int MAX_OFFSET_MILLIS = 500;

    protected static BoundedThreadPool boundedThreadPool
            = new BoundedThreadPool(Runtime.getRuntime().availableProcessors() * 10);

    public static void waitRandom() {
        try {
            Thread.sleep(RANDOM.nextLong(50, 1000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void beforeAll() {
    }

    @AfterAll
    public static void afterAll() {
    }

    @BeforeEach
    public void beforeEachTest() {
        super.beforeEachTest();
    }

    @AfterEach
    public void afterEachTest() {
        super.afterEachTest();
    }

    protected static <T> T awaitFuture(Future<T> future) throws SQLException {
        try {
            return boundedThreadPool.awaitFuture(future);
        } catch (TimeoutException e) {
            throw new SQLException("Timeout or execution exception", e);
        } catch (ExecutionException e) {
            throw new SQLException("Execution exception", e.getCause());
        }
    }
}
