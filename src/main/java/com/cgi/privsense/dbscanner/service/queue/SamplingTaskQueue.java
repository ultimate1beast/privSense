package com.cgi.privsense.dbscanner.service.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Thread-safe queue for sampling tasks.
 * Provides a mechanism for producers to submit tasks and track their status.
 */
@Component
public class SamplingTaskQueue implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(SamplingTaskQueue.class);

    private final BlockingQueue<SamplingTask> taskQueue;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger activeProducers = new AtomicInteger(0);
    private final AtomicInteger pendingTasks = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param queueCapacity Maximum capacity of the queue
     */
    public SamplingTaskQueue(@Value("${dbscanner.queue.capacity:1000}") int queueCapacity) {
        this.taskQueue = new LinkedBlockingQueue<>(queueCapacity);
        log.info("Sampling task queue initialized with capacity {}", queueCapacity);
    }

    /**
     * Adds a task to the queue.
     * Blocks if the queue is full.
     *
     * @param task Task to add
     * @throws InterruptedException If the thread is interrupted while waiting
     * @throws IllegalStateException If the queue is shutdown
     */
    public void addTask(SamplingTask task) throws InterruptedException {
        if (isShutdown.get()) {
            throw new IllegalStateException("Queue is shutdown");
        }

        activeProducers.incrementAndGet();
        try {
            taskQueue.put(task);
            pendingTasks.incrementAndGet();
            log.debug("Added task to queue: {}", task);
        } finally {
            activeProducers.decrementAndGet();
        }
    }

    /**
     * Takes a task from the queue.
     * Blocks until a task is available or timeout is reached.
     *
     * @param timeout Timeout value
     * @param unit Timeout unit
     * @return Task, or null if timeout occurred or queue is empty and no producers are active
     * @throws InterruptedException If the thread is interrupted while waiting
     */
    public SamplingTask takeTask(long timeout, TimeUnit unit) throws InterruptedException {
        SamplingTask task = taskQueue.poll(timeout, unit);

        if (task != null) {
            pendingTasks.decrementAndGet();
            return task;
        }

        // If no active producers and queue is empty, return null
        if (activeProducers.get() == 0 && taskQueue.isEmpty()) {
            return null;
        }

        // Otherwise, timeout occurred but producers might still be active
        return null;
    }

    /**
     * Gets the number of pending tasks.
     *
     * @return Number of pending tasks
     */
    public int getPendingTaskCount() {
        return pendingTasks.get();
    }

    /**
     * Gets the number of active producers.
     *
     * @return Number of active producers
     */
    public int getActiveProducerCount() {
        return activeProducers.get();
    }

    /**
     * Checks if there are pending tasks or active producers.
     *
     * @return true if there are pending tasks or active producers
     */
    public boolean hasActiveTasks() {
        return pendingTasks.get() > 0 || activeProducers.get() > 0;
    }

    /**
     * Signals the start of producing tasks.
     */
    public void startProducing() {
        activeProducers.incrementAndGet();
    }

    /**
     * Signals the end of producing tasks.
     */
    public void finishProducing() {
        activeProducers.decrementAndGet();
    }

    /**
     * Shuts down the queue.
     * After calling this method, no more tasks can be added.
     */
    public void shutdown() {
        log.info("Shutting down sampling task queue");
        isShutdown.set(true);
    }

    /**
     * Disposes the queue.
     * Called by Spring when the bean is destroyed.
     */
    @Override
    public void destroy() {
        shutdown();
    }
}