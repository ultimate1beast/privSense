package com.cgi.privsense.dbscanner.service;

import com.cgi.privsense.dbscanner.core.datasource.DataSourceProvider;
import com.cgi.privsense.dbscanner.exception.SamplingException;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.service.queue.SamplingTask;
import com.cgi.privsense.dbscanner.service.queue.SamplingTaskProcessor;
import com.cgi.privsense.dbscanner.service.queue.SamplingTaskQueue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Service for parallel sampling of database data.
 * Optimized to use producer-consumer pattern with queues for better resource utilization.
 */
@Slf4j
@Service
public class OptimizedParallelSamplingService {

    /**
     * Provider for data sources.
     */
    private final DataSourceProvider dataSourceProvider;

    /**
     * Maximum number of threads in the direct pool.
     */
    private final int maxThreads;

    /**
     * Default timeout for operations.
     */
    private final long defaultTimeout;

    /**
     * Timeout unit.
     */
    private final TimeUnit timeoutUnit;

    /**
     * Sampling task queue.
     */
    private final SamplingTaskQueue taskQueue;

    /**
     * Sampling task processor.
     */
    private final SamplingTaskProcessor taskProcessor;

    /**
     * Whether to use the queue for sampling.
     */
    private final boolean useQueueForSampling;

    /**
     * Constructor.
     *
     * @param dataSourceProvider Provider for data sources
     * @param taskQueue Sampling task queue
     * @param taskProcessor Sampling task processor
     * @param maxThreads Maximum number of threads
     * @param defaultTimeout Default timeout
     * @param timeoutUnit Timeout unit
     * @param useQueueForSampling Whether to use the queue for sampling
     */
    public OptimizedParallelSamplingService(
            DataSourceProvider dataSourceProvider,
            SamplingTaskQueue taskQueue,
            SamplingTaskProcessor taskProcessor,
            @Value("${dbscanner.threads.max-pool-size:#{T(java.lang.Runtime).getRuntime().availableProcessors()}}") int maxThreads,
            @Value("${dbscanner.sampling.timeout:30}") long defaultTimeout,
            @Value("${dbscanner.sampling.timeout-unit:SECONDS}") TimeUnit timeoutUnit,
            @Value("${dbscanner.sampling.use-queue:true}") boolean useQueueForSampling) {

        this.dataSourceProvider = dataSourceProvider;
        this.taskQueue = taskQueue;
        this.taskProcessor = taskProcessor;
        this.maxThreads = maxThreads;
        this.defaultTimeout = defaultTimeout;
        this.timeoutUnit = timeoutUnit;
        this.useQueueForSampling = useQueueForSampling;

        log.info("Initialized parallel sampling service with {} threads, using queue: {}",
                maxThreads, useQueueForSampling);
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
    public DataSample sampleTable(String dbType, String connectionId, String tableName, int limit) {
        StopWatch watch = new StopWatch();
        watch.start();

        DataSource dataSource = dataSourceProvider.getDataSource(connectionId);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT * FROM " + escapeIdentifier(tableName, dbType) + " LIMIT ?")) {

            stmt.setInt(1, limit);
            List<Map<String, Object>> rows = new ArrayList<>();

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    rows.add(row);
                }
            }

            watch.stop();
            log.debug("Sampled table {} in {} ms", tableName, watch.getTotalTimeMillis());
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
    public List<Object> sampleColumn(String dbType, String connectionId, String tableName, String columnName, int limit) {
        StopWatch watch = new StopWatch();
        watch.start();

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

            watch.stop();
            log.debug("Sampled column {}.{} in {} ms", tableName, columnName, watch.getTotalTimeMillis());
            return values;
        } catch (SQLException e) {
            throw new SamplingException("Error sampling column: " + tableName + "." + columnName, e);
        }
    }

    /**
     * Samples data from multiple columns in parallel.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnNames List of column names
     * @param limit Maximum number of values per column
     * @return Map of column name to list of sampled values
     */
    public Map<String, List<Object>> sampleColumnsInParallel(String dbType, String connectionId,
                                                             String tableName, List<String> columnNames, int limit) {
        StopWatch watch = new StopWatch();
        watch.start();

        // First, validate that columns exist in the table
        DataSource dataSource = dataSourceProvider.getDataSource(connectionId);
        List<String> existingColumns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            // Get metadata about the table columns
            List<String> tableColumns = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT * FROM " + escapeIdentifier(tableName, dbType) + " LIMIT 0");
                 ResultSet rs = stmt.executeQuery()) {

                ResultSetMetaData metaData = rs.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    tableColumns.add(columnName.toLowerCase());
                    log.debug("Found column in table {}: {}", tableName, columnName);
                }
            }

            // Filter requested columns to only include ones that exist
            for (String column : columnNames) {
                if (column == null || column.trim().isEmpty()) {
                    log.warn("Ignoring null or empty column name");
                    continue;
                }

                if (tableColumns.contains(column.toLowerCase())) {
                    existingColumns.add(column);
                } else {
                    log.warn("Column '{}' does not exist in table '{}', skipping", column, tableName);
                }
            }
        } catch (SQLException e) {
            log.error("Error validating columns: {}", e.getMessage());
            // Fall back to using all requested columns
            existingColumns = new ArrayList<>(columnNames);
        }

        if (existingColumns.isEmpty()) {
            log.warn("No valid columns found for sampling in table: {}", tableName);
            return new HashMap<>();
        }

        // For a small number of columns, use a single query
        if (existingColumns.size() <= 3) {
            Map<String, List<Object>> result = sampleColumnsWithSingleQuery(dbType, connectionId, tableName, existingColumns, limit);
            watch.stop();
            log.debug("Sampled {} columns with single query in {} ms", existingColumns.size(), watch.getTotalTimeMillis());
            return result;
        } else {
            // Choose approach based on configuration
            if (useQueueForSampling) {
                Map<String, List<Object>> result = sampleColumnsWithQueue(dbType, connectionId, tableName, existingColumns, limit);
                watch.stop();
                log.debug("Sampled {} columns with queue in {} ms", existingColumns.size(), watch.getTotalTimeMillis());
                return result;
            } else {
                Map<String, List<Object>> result = sampleColumnsWithDirectParallelism(dbType, connectionId, tableName, existingColumns, limit);
                watch.stop();
                log.debug("Sampled {} columns with direct parallelism in {} ms", existingColumns.size(), watch.getTotalTimeMillis());
                return result;
            }
        }
    }

    /**
     * Samples data from multiple columns using the queue.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnNames List of column names
     * @param limit Maximum number of rows
     * @return Map of column name to list of sampled values
     */
    private Map<String, List<Object>> sampleColumnsWithQueue(String dbType, String connectionId,
                                                             String tableName, List<String> columnNames, int limit) {
        Map<String, List<Object>> results = new ConcurrentHashMap<>();
        CountDownLatch completionLatch = new CountDownLatch(columnNames.size());

        try {
            // Signal start of producing tasks
            taskQueue.startProducing();

            try {
                // Create tasks for all columns
                for (String columnName : columnNames) {
                    // Create completion callback
                    Consumer<List<Object>> callback = samples -> {
                        results.put(columnName, samples);
                        completionLatch.countDown();
                    };

                    // Create and submit task
                    SamplingTask task = new SamplingTask(
                            dbType, connectionId, tableName, columnName, limit, callback);

                    taskQueue.addTask(task);
                }
            } finally {
                // Signal end of producing tasks
                taskQueue.finishProducing();
            }

            // Wait for all tasks to complete
            if (!completionLatch.await(defaultTimeout, timeoutUnit)) {
                log.warn("Timeout waiting for column sampling tasks to complete");
            }

            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sampling columns", e);
            return results; // Return partial results
        }
    }

    /**
     * Samples data from multiple columns with parallel queries using a direct thread pool.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnNames List of column names
     * @param limit Maximum number of values per column
     * @return Map of column name to list of sampled values
     */
    private Map<String, List<Object>> sampleColumnsWithDirectParallelism(String dbType, String connectionId,
                                                                         String tableName, List<String> columnNames, int limit) {
        // Calculate optimal number of threads
        int numThreads = Math.min(columnNames.size(), maxThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads, r -> {
            Thread t = new Thread(r);
            t.setName("direct-sampler-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        try {
            // Submit a task for each column
            List<Future<Map.Entry<String, List<Object>>>> futures = new ArrayList<>();

            for (String columnName : columnNames) {
                futures.add(executor.submit(() -> {
                    // Get a connection from the pool
                    List<Object> values = sampleColumn(dbType, connectionId, tableName, columnName, limit);
                    return Map.entry(columnName, values);
                }));
            }

            // Collect results
            Map<String, List<Object>> results = new ConcurrentHashMap<>();
            for (Future<Map.Entry<String, List<Object>>> future : futures) {
                try {
                    Map.Entry<String, List<Object>> entry = future.get(defaultTimeout, timeoutUnit);
                    results.put(entry.getKey(), entry.getValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted while sampling columns", e);
                } catch (ExecutionException e) {
                    log.error("Error sampling column: {}", e.getCause().getMessage(), e.getCause());
                } catch (TimeoutException e) {
                    log.error("Timeout sampling column", e);
                }
            }

            return results;
        } finally {
            // Shutdown the executor service
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    /**
     * Samples data from multiple columns with a single query.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnNames List of column names
     * @param limit Maximum number of rows
     * @return Map of column name to list of sampled values
     */
    private Map<String, List<Object>> sampleColumnsWithSingleQuery(String dbType, String connectionId,
                                                                   String tableName, List<String> columnNames, int limit) {
        DataSource dataSource = dataSourceProvider.getDataSource(connectionId);

        String columns = columnNames.stream()
                .map(column -> escapeIdentifier(column, dbType))
                .collect(Collectors.joining(", "));

        String sql = String.format("SELECT %s FROM %s LIMIT %d",
                columns, escapeIdentifier(tableName, dbType), limit);

        log.debug("Executing SQL query: {}", sql);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            Map<String, List<Object>> columnData = new HashMap<>();
            for (String column : columnNames) {
                columnData.put(column, new ArrayList<>());
            }

            try (ResultSet rs = stmt.executeQuery()) {
                // Create a mapping of column names to their indices
                Map<String, Integer> columnIndices = new HashMap<>();
                ResultSetMetaData metaData = rs.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnLabel = metaData.getColumnLabel(i);
                    // Remove any backticks or other database-specific quoting
                    String cleanColumnName = columnLabel.replaceAll("[`\"\\[\\]]", "");
                    log.debug("Column at index {}: label='{}', clean name='{}'", i, columnLabel, cleanColumnName);

                    // Store both the original and clean column names for robustness
                    columnIndices.put(columnLabel, i);
                    columnIndices.put(cleanColumnName, i);
                }

                while (rs.next()) {
                    for (String column : columnNames) {
                        try {
                            // Try to get by index first (most reliable)
                            Integer index = columnIndices.get(column);
                            if (index != null) {
                                Object value = rs.getObject(index);
                                columnData.get(column).add(value);
                            } else {
                                // Fallback: try direct access by column name
                                // (may fail if column names don't match exactly)
                                Object value = rs.getObject(column);
                                columnData.get(column).add(value);
                                log.debug("Accessed column '{}' directly by name", column);
                            }
                        } catch (SQLException e) {
                            log.warn("Failed to access column '{}': {}", column, e.getMessage());
                            // Add null for this column to maintain consistency
                            columnData.get(column).add(null);
                        }
                    }
                }
            }

            return columnData;
        } catch (SQLException e) {
            log.error("SQL error during sampling: {}", e.getMessage());
            throw new SamplingException("Error sampling columns with single query", e);
        }
    }

    /**
     * Samples data from multiple tables in parallel.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableNames List of table names
     * @param limit Maximum number of rows per table
     * @return Map of table name to data sample
     */
    public Map<String, DataSample> sampleTablesInParallel(String dbType, String connectionId,
                                                          List<String> tableNames, int limit) {
        StopWatch watch = new StopWatch();
        watch.start();

        // Choose approach based on configuration
        if (useQueueForSampling) {
            Map<String, DataSample> result = sampleTablesWithQueue(dbType, connectionId, tableNames, limit);
            watch.stop();
            log.debug("Sampled {} tables with queue in {} ms", tableNames.size(), watch.getTotalTimeMillis());
            return result;
        } else {
            Map<String, DataSample> result = sampleTablesWithDirectParallelism(dbType, connectionId, tableNames, limit);
            watch.stop();
            log.debug("Sampled {} tables with direct parallelism in {} ms", tableNames.size(), watch.getTotalTimeMillis());
            return result;
        }
    }

    /**
     * Samples data from multiple tables using the queue.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableNames List of table names
     * @param limit Maximum number of rows
     * @return Map of table name to data sample
     */
    private Map<String, DataSample> sampleTablesWithQueue(String dbType, String connectionId,
                                                          List<String> tableNames, int limit) {
        Map<String, DataSample> results = new ConcurrentHashMap<>();
        CountDownLatch completionLatch = new CountDownLatch(tableNames.size());

        try {
            // Signal start of producing tasks
            taskQueue.startProducing();

            try {
                // Create tasks for all tables
                for (String tableName : tableNames) {
                    // Create completion callback
                    Consumer<DataSample> callback = sample -> {
                        results.put(tableName, sample);
                        completionLatch.countDown();
                    };

                    // Create and submit task
                    SamplingTask task = new SamplingTask(
                            dbType, connectionId, tableName, limit, callback);

                    taskQueue.addTask(task);
                }
            } finally {
                // Signal end of producing tasks
                taskQueue.finishProducing();
            }

            // Wait for all tasks to complete
            if (!completionLatch.await(defaultTimeout, timeoutUnit)) {
                log.warn("Timeout waiting for table sampling tasks to complete");
            }

            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sampling tables", e);
            return results; // Return partial results
        }
    }

    /**
     * Samples data from multiple tables with direct parallel queries.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableNames List of table names
     * @param limit Maximum number of rows per table
     * @return Map of table name to data sample
     */
    private Map<String, DataSample> sampleTablesWithDirectParallelism(String dbType, String connectionId,
                                                                      List<String> tableNames, int limit) {
        // Calculate optimal number of threads
        int numThreads = Math.min(tableNames.size(), maxThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads, r -> {
            Thread t = new Thread(r);
            t.setName("direct-table-sampler-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        try {
            // Submit a task for each table
            List<Future<Map.Entry<String, DataSample>>> futures = new ArrayList<>();

            for (String tableName : tableNames) {
                futures.add(executor.submit(() -> {
                    DataSample sample = sampleTable(dbType, connectionId, tableName, limit);
                    return Map.entry(tableName, sample);
                }));
            }

            // Collect results
            Map<String, DataSample> results = new ConcurrentHashMap<>();
            for (Future<Map.Entry<String, DataSample>> future : futures) {
                try {
                    Map.Entry<String, DataSample> entry = future.get(defaultTimeout, timeoutUnit);
                    results.put(entry.getKey(), entry.getValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted while sampling tables", e);
                } catch (ExecutionException e) {
                    log.error("Error sampling table: {}", e.getCause().getMessage(), e.getCause());
                } catch (TimeoutException e) {
                    log.error("Timeout sampling table", e);
                }
            }

            return results;
        } finally {
            // Shutdown the executor service
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
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
}