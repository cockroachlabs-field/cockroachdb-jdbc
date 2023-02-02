package io.cockroachdb.jdbc.it.util;

import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundedThreadPool {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ThreadPoolExecutor threadPool;

    public BoundedThreadPool(int corePoolSize) {
        this.threadPool = new ThreadPoolExecutor(
                corePoolSize, corePoolSize,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(Integer.MAX_VALUE),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public void shutdownAndDrain() {
        logger.debug("Shutting down thread tool [{}]", threadPool);
        threadPool.shutdownNow();

        try {
            while (!threadPool.awaitTermination(2, TimeUnit.SECONDS)) {
                logger.debug("Awaiting active thread completion [{}]", threadPool);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return submitWithDelay(callable, 0);
    }

    public <T> Future<T> submitWithDelay(Callable<T> callable, long waitTimeMillis) {
        if (threadPool.isShutdown() || threadPool.isTerminated()) {
            throw new IllegalStateException("Thread pool shutdown");
        }

        return threadPool.submit(() -> {
            try {
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
            return callable.call();
        });
    }

    public <T> Future<T> submitAndWait(Callable<T> callable, long waitTimeMillis) {
        if (threadPool.isShutdown() || threadPool.isTerminated()) {
            throw new IllegalStateException("Thread pool shutdown");
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Future<T> f = threadPool.submit(() -> {
            countDownLatch.countDown();
            return callable.call();
        });
        if (waitTimeMillis > 0) {
            try {
                countDownLatch.await();
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        }
        return f;
    }

    public <T> T awaitFuture(Future<T> future) throws ExecutionException, TimeoutException {
        try {
            return future.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }
}
