package com.cgi.privsense.dbscanner.service;

import com.cgi.privsense.dbscanner.core.datasource.DataSourceProvider;
import com.cgi.privsense.dbscanner.exception.SamplingException;
import com.cgi.privsense.dbscanner.model.DataSample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service for parallel sampling of database data.
 * Optimized to use direct JDBC connections for maximum performance.
 */
@Slf4j
@Service
public class OptimizedParallelSamplingService {

    /**
     * Provider for data sources.
     */
    private final DataSourceProvider dataSourceProvider;

    /**
     * Maximum number of threads in the pool.
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
     * Constructor.
     *
     * @param dataSourceProvider Provider for data sources
     * @param maxThreads Maximum number of threads
     * @param defaultTimeout Default timeout
     * @param timeoutUnit Timeout unit
     */
    public OptimizedParallelSamplingService(
            DataSourceProvider dataSourceProvider,
            @Value("${dbscanner.threads.max-pool-size:#{T(java.lang.Runtime).getRuntime().availableProcessors()}}") int maxThreads,
            @Value("${dbscanner.sampling.timeout:30}") long defaultTimeout,
            @Value("${dbscanner.sampling.timeout-unit:SECONDS}") TimeUnit timeoutUnit) {
        this.dataSourceProvider = dataSourceProvider;
        this.maxThreads = maxThreads;
        this.defaultTimeout = defaultTimeout;
        this.timeoutUnit = timeoutUnit;

        log.info("Initialized parallel sampling service with {} threads", maxThreads);
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
    public List<Object> sampleColumn(String dbType, String connectionId, String tableName, String columnName, int limit) {
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
            return sampleColumnsWithSingleQuery(dbType, connectionId, tableName, existingColumns, limit);
        } else {
            return sampleColumnsWithParallelism(dbType, connectionId, tableName, existingColumns, limit);
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
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String columnLabel = rs.getMetaData().getColumnLabel(i);
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
     * Samples data from multiple columns with parallel queries.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnNames List of column names
     * @param limit Maximum number of values per column
     * @return Map of column name to list of sampled values
     */
    private Map<String, List<Object>> sampleColumnsWithParallelism(String dbType, String connectionId,
                                                                   String tableName, List<String> columnNames, int limit) {
        // Calculate optimal number of threads
        int numThreads = Math.min(columnNames.size(), maxThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

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
            executor.shutdownNow();
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
        // Calculate optimal number of threads
        int numThreads = Math.min(tableNames.size(), maxThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

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
            executor.shutdownNow();
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