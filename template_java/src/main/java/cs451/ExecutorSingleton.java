package cs451;

import java.util.concurrent.*;

public class ExecutorSingleton {
    private static final ExecutorService threadPool = Executors.newCachedThreadPool();
    public static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Constants.SCHEDULER_THREADS);

    public static Future<?> submit(Runnable runnable) {
        try {
            return threadPool.submit(runnable);
        } catch (RejectedExecutionException ignored) {
            return null;
        }
    }

    public static void shutdown() {
        threadPool.shutdown();
        scheduler.shutdown();
    }
}
