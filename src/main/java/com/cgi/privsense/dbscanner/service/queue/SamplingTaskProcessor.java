package com.cgi.privsense.dbscanner.service.queue;

import com.cgi.privsense.dbscanner.exception.SamplingException;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.core.datasource.DataSourceProvider;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processor for sampling tasks.
 * Uses a thread pool to process tasks from the queue.
 */
@Component
public class SamplingTaskProcessor implements DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(SamplingTaskProcessor.class);

    private final SamplingTaskQueue taskQueue;
    private final DataSourceProvider dataSourceProvider;
    private final ExecutorService executorService;
    private final int numThreads;
    private final long pollTimeout;
    private final TimeUnit pollTimeoutUnit;
    private final AtomicInteger processedTaskCount = new AtomicInteger(0);
    private final AtomicInteger failedTaskCount = new AtomicInteger(0);
    private final AtomicInteger activeThreadCount = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param taskQueue Task queue
     * @param dataSourceProvider Data source provider
     * @param numThreads Number of threads
     * @param pollTimeout Timeout for polling the queue
     * @param pollTimeoutUnit Timeout unit for polling the queue
     */
    public SamplingTaskProcessor(
            SamplingTaskQueue taskQueue,
            DataSourceProvider dataSourceProvider,
            @Value("${dbscanner.threads.sampler-pool-size:4}") int numThreads,
            @Value("${dbscanner.queue.poll-timeout:500}") long pollTimeout,
            @Value("${dbscanner.queue.poll-timeout-unit:MILLISECONDS}") TimeUnit pollTimeoutUnit) {

        this.taskQueue = taskQueue;
        this.dataSourceProvider = dataSourceProvider;
        this.numThreads = numThreads;
        this.pollTimeout = pollTimeout;
        this.pollTimeoutUnit = pollTimeoutUnit;

        // Create thread pool with custom thread factory
        this.executorService = Executors.newFixedThreadPool(numThreads, r -> {
            Thread t = new Thread(r);
            t.setName("sampler-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        log.info("Sampling task processor initialized with {} threads", numThreads);
    }

    /**
     * Initializes and starts the consumer threads.
     */
    @PostConstruct
    public void init() {
        // Start consumer threads
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(this::processTasksLoop);
        }
        log.info("Started {} consumer threads for database sampling", numThreads);
    }

    /**
     * Main processing loop for consumer threads.
     */
    private void processTasksLoop() {
        activeThreadCount.incrementAndGet();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // Take a task from the queue
                    SamplingTask task = taskQueue.takeTask(pollTimeout, pollTimeoutUnit);

                    if (task == null) {
                        // No task available, check if we should continue
                        if (!taskQueue.hasActiveTasks() && executorService.isShutdown()) {
                            // No more tasks and we're shutting down
                            break;
                        }
                        // Otherwise, continue polling
                        continue;
                    }

                    // Process the task
                    processTask(task);
                    processedTaskCount.incrementAndGet();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("Consumer thread interrupted, exiting loop");
                    break;
                } catch (Exception e) {
                    failedTaskCount.incrementAndGet();
                    log.error("Error processing sampling task: {}", e.getMessage(), e);
                    // Continue processing other tasks
                }
            }
        } finally {
            activeThreadCount.decrementAndGet();
            log.debug("Consumer thread exiting, active threads: {}", activeThreadCount.get());
        }
    }

    /**
     * Processes a single sampling task.
     *
     * @param task Task to process
     */
    private void processTask(SamplingTask task) {
        if (task.isTableSamplingTask()) {
            // Process table sampling task
            try {
                DataSample sample = sampleTable(
                        task.getDbType(),
                        task.getConnectionId(),
                        task.getTableName(),
                        task.getLimit());

                // Invoke callback
                if (task.getTableCallback() != null) {
                    task.getTableCallback().accept(sample);
                }

                log.debug("Completed table sampling task: {}", task.getTableName());
            } catch (Exception e) {
                log.error("Error sampling table {}: {}", task.getTableName(), e.getMessage(), e);
                // Call callback with null to prevent hanging
                if (task.getTableCallback() != null) {
                    task.getTableCallback().accept(null);
                }
                throw new SamplingException("Error sampling table: " + task.getTableName(), e);
            }
        } else if (task.isColumnSamplingTask()) {
            // Process column sampling task
            try {
                List<Object> samples = sampleColumn(
                        task.getDbType(),
                        task.getConnectionId(),
                        task.getTableName(),
                        task.getColumnName(),
                        task.getLimit());

                // Invoke callback
                if (task.getColumnCallback() != null) {
                    task.getColumnCallback().accept(samples);
                }

                log.debug("Completed column sampling task: {}.{}", task.getTableName(), task.getColumnName());
            } catch (Exception e) {
                log.error("Error sampling column {}.{}: {}",
                        task.getTableName(), task.getColumnName(), e.getMessage(), e);
                // Call callback with empty list to prevent hanging
                if (task.getColumnCallback() != null) {
                    task.getColumnCallback().accept(Collections.emptyList());
                }
                throw new SamplingException("Error sampling column: " + task.getTableName() + "." + task.getColumnName(), e);
            }
        } else {
            log.warn("Invalid task type: {}", task);
        }
    }

    /**
     * Samples data from a table.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param limit Maximum number of rows
     * @return Data sample
     */
    private DataSample sampleTable(String dbType, String connectionId, String tableName, int limit) {
        DataSource dataSource = dataSourceProvider.getDataSource(connectionId);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT * FROM " + escapeIdentifier(tableName, dbType) + " LIMIT ?")) {

            stmt.setInt(1, limit);
            List<Map<String, Object>> rows = new ArrayList<>();

            try (ResultSet rs = stmt.executeQuery()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rs.getMetaData().getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    rows.add(row);
                }
            }

            return DataSample.fromRows(tableName, rows);
        } catch (SQLException e) {
            throw new SamplingException("Error sampling table: " + tableName, e);
        }
    }

    /**
     * Samples data from a column.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnName Column name
     * @param limit Maximum number of values
     * @return List of sampled values
     */
    private List<Object> sampleColumn(String dbType, String connectionId, String tableName, String columnName, int limit) {
        DataSource dataSource = dataSourceProvider.getDataSource(connectionId);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT " + escapeIdentifier(columnName, dbType) +
                             " FROM " + escapeIdentifier(tableName, dbType) + " LIMIT ?")) {

            stmt.setInt(1, limit);
            List<Object> values = new ArrayList<>();

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    values.add(rs.getObject(1));
                }
            }

            return values;
        } catch (SQLException e) {
            throw new SamplingException("Error sampling column: " + tableName + "." + columnName, e);
        }
    }

    /**
     * Escapes an identifier according to the database type.
     *
     * @param identifier Identifier to escape
     * @param dbType Database type
     * @return Escaped identifier
     */
    private String escapeIdentifier(String identifier, String dbType) {
        switch (dbType.toLowerCase()) {
            case "mysql":
                return "`" + identifier.replace("`", "``") + "`";
            case "postgresql", "oracle":
                return "\"" + identifier.replace("\"", "\"\"") + "\"";
            case "sqlserver":
                return "[" + identifier.replace("]", "]]") + "]";
            default:
                return identifier;
        }
    }

    /**
     * Gets the number of processed tasks.
     *
     * @return Number of processed tasks
     */
    public int getProcessedTaskCount() {
        return processedTaskCount.get();
    }

    /**
     * Gets the number of failed tasks.
     *
     * @return Number of failed tasks
     */
    public int getFailedTaskCount() {
        return failedTaskCount.get();
    }

    /**
     * Gets the number of active threads.
     *
     * @return Number of active threads
     */
    public int getActiveThreadCount() {
        return activeThreadCount.get();
    }

    /**
     * Shuts down the processor.
     */
    public void shutdown() {
        log.info("Shutting down sampling task processor");

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }

        log.info("Sampling task processor shutdown complete");
    }

    /**
     * Disposes the processor.
     * Called by Spring when the bean is destroyed.
     */
    @Override
    public void destroy() {
        shutdown();
    }
}