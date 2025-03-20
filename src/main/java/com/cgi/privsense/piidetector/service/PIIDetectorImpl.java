/*
 * PIIDetectorImpl.java - Main implementation of the PII detector
 */
package com.cgi.privsense.piidetector.service;

import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;
import com.cgi.privsense.dbscanner.service.OptimizedParallelSamplingService;
import com.cgi.privsense.dbscanner.service.ScannerService;

import com.cgi.privsense.piidetector.api.PIIDetectionStrategyFactory;
import com.cgi.privsense.piidetector.api.PIIDetector;
import com.cgi.privsense.piidetector.api.PIIReportGenerator;
import com.cgi.privsense.piidetector.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Main implementation of the PII detector.
 * Uses a pipeline approach to sequentially apply detection strategies.
 */
@Service
public class PIIDetectorImpl implements PIIDetector {
    private static final Logger log = LoggerFactory.getLogger(PIIDetectorImpl.class);

    private final ScannerService scannerService;
    private final OptimizedParallelSamplingService samplingService;
    private final PIIDetectionStrategyFactory strategyFactory;
    private final PIIReportGenerator reportGenerator;
    private final PIIDetectionPipelineService pipelineService;
    private final PIIDetectionMetricsCollector metricsCollector;
    private final DetectionResultCleaner resultCleaner;

    private double confidenceThreshold = 0.7;
    private int sampleSize = 10;
    private Map<String, Boolean> activeStrategies = new ConcurrentHashMap<>();
    private Map<String, Integer> tableSampleSizes = new ConcurrentHashMap<>();
    private int maxThreads;

    // Cache for table results to avoid redundant processing
    private final Map<String, TablePIIInfo> tableResultCache = new ConcurrentHashMap<>();

    // Token bucket for rate limiting resource-intensive operations
    private final TokenBucket nerServiceTokenBucket = new TokenBucket(5, 5, TimeUnit.SECONDS);

    @Autowired
    public PIIDetectorImpl(
            ScannerService scannerService,
            OptimizedParallelSamplingService samplingService,
            PIIDetectionStrategyFactory strategyFactory,
            PIIReportGenerator reportGenerator,
            PIIDetectionPipelineService pipelineService,
            PIIDetectionMetricsCollector metricsCollector,
            DetectionResultCleaner resultCleaner,
            @Value("${piidetector.threads.max-pool-size:#{T(java.lang.Runtime).getRuntime().availableProcessors()}}") int maxThreads) {

        this.scannerService = scannerService;
        this.samplingService = samplingService;
        this.strategyFactory = strategyFactory;
        this.reportGenerator = reportGenerator;
        this.pipelineService = pipelineService;
        this.metricsCollector = metricsCollector;
        this.resultCleaner = resultCleaner;
        this.maxThreads = maxThreads;

        // Default: enable all strategies
        strategyFactory.getAllStrategies().forEach(strategy -> {
            activeStrategies.put(strategy.getName(), true);
        });
    }

    @Override
    public PIIDetectionResult detectPII(String connectionId, String dbType) {
        // Reset metrics at the start of each complete analysis
        metricsCollector.resetMetrics();

        // Clear relevant caches for fresh analysis
        ((PIIDetectionStrategyFactoryImpl)strategyFactory).clearCompositeCache();
        ((PIIDetectionPipelineService)pipelineService).clearCache();
        tableResultCache.clear();

        long startTime = System.currentTimeMillis();

        log.info("Starting PII detection for connection: {}, type: {}", connectionId, dbType);

        PIIDetectionResult result = PIIDetectionResult.builder()
                .connectionId(connectionId)
                .dbType(dbType)
                .tableResults(new ArrayList<>())
                .piiTypeCounts(new HashMap<>())
                .additionalMetadata(new HashMap<>()) // Explicitly initialize additionalMetadata
                .build();

        // Get all tables
        List<TableMetadata> tables = scannerService.scanTables(dbType, connectionId);
        log.info("Number of tables found: {}", tables.size());

        // Configure adaptive sample sizes for tables
        configureAdaptiveSampleSizes(connectionId, dbType, tables);

        // Create an optimized thread pool for better resource utilization
        int optimalThreads = Math.min(tables.size(), Runtime.getRuntime().availableProcessors() * 2);
        ExecutorService executor = new ForkJoinPool(optimalThreads,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, true);

        List<Future<TablePIIInfo>> futures = new ArrayList<>();

        // Submit each table for analysis, prioritizing smaller tables first for quick wins
        List<TableMetadata> sortedTables = new ArrayList<>(tables);
        sortedTables.sort(Comparator.comparingInt(table -> {
            try {
                // Try to get column count as a simple size estimate
                return scannerService.scanColumns(dbType, connectionId, table.getName()).size();
            } catch (Exception e) {
                return Integer.MAX_VALUE; // Put tables with errors at the end
            }
        }));

        // Submit work using a completion service to process results as they complete
        CompletionService<TablePIIInfo> completionService = new ExecutorCompletionService<>(executor);
        for (TableMetadata table : sortedTables) {
            futures.add(completionService.submit(() -> detectPIIInTable(connectionId, dbType, table.getName())));
        }

        // Collect results as they complete
        for (int i = 0; i < futures.size(); i++) {
            try {
                TablePIIInfo tableResult = completionService.take().get(10, TimeUnit.MINUTES);
                result.addTableResult(tableResult);

                // Log progress periodically
                if ((i + 1) % 10 == 0 || i == futures.size() - 1) {
                    log.info("Progress: {}/{} tables processed", i + 1, futures.size());
                }
            } catch (Exception e) {
                log.error("Error while analyzing a table: {}", e.getMessage(), e);
            }
        }

        executor.shutdown();

        long endTime = System.currentTimeMillis();
        result.setProcessingTimeMs(endTime - startTime);

        log.info("PII detection completed. Total time: {} ms. PIIs detected: {}",
                result.getProcessingTimeMs(), result.getTotalPiiCount());

        // Add additional metrics to the result
        // Verify that additionalMetadata is not null before accessing it
        if (result.getAdditionalMetadata() == null) {
            result.setAdditionalMetadata(new HashMap<>());
        }
        result.getAdditionalMetadata().put("performanceMetrics", metricsCollector.getMetricsReport());

        // Display metrics report at the end
        metricsCollector.logMetricsReport();

        return result;
    }

    @Override
    public TablePIIInfo detectPIIInTable(String connectionId, String dbType, String tableName) {
        String cacheKey = connectionId + ":" + dbType + ":" + tableName;

        // Check cache first
        if (tableResultCache.containsKey(cacheKey)) {
            log.debug("Using cached result for table {}", tableName);
            return tableResultCache.get(cacheKey);
        }

        long tableStartTime = System.currentTimeMillis();
        log.info("Analyzing table: {}", tableName);

        TablePIIInfo tableResult = TablePIIInfo.builder()
                .tableName(tableName)
                .columnResults(new ArrayList<>())
                .build();

        // Get table metadata
        List<ColumnMetadata> columns = scannerService.scanColumns(dbType, connectionId, tableName);

        // Get the appropriate sample size for this table
        int effectiveSampleSize = tableSampleSizes.getOrDefault(tableName, sampleSize);
        log.debug("Using sample size {} for table {}", effectiveSampleSize, tableName);

        // Pre-fetch batches of sample data for efficiency
        Map<String, List<Object>> columnSamples = new HashMap<>();
        try {
            columnSamples = batchSampleColumns(connectionId, dbType, tableName,
                    columns.stream()
                            .map(ColumnMetadata::getName)
                            .collect(Collectors.toList()),
                    effectiveSampleSize);
        } catch (Exception e) {
            log.warn("Error while batch sampling table {}: {}", tableName, e.getMessage());
            // Continue with empty samples, will fetch individually as needed
        }

        // Process columns in batch sizes to limit memory usage
        List<List<ColumnMetadata>> columnBatches = partitionList(columns, 20);

        for (List<ColumnMetadata> batch : columnBatches) {
            // For each column in this batch
            for (ColumnMetadata column : batch) {
                try {
                    String columnName = column.getName();

                    // Get pre-fetched samples or fetch individually if not available
                    List<Object> samples = columnSamples.getOrDefault(columnName, null);
                    if (samples == null) {
                        try {
                            samples = samplingService.sampleColumn(dbType, connectionId, tableName,
                                    columnName, effectiveSampleSize);
                        } catch (Exception e) {
                            log.warn("Error sampling column {}.{}: {}", tableName, columnName, e.getMessage());
                            samples = Collections.emptyList();
                        }
                    }

                    // Process the column
                    ColumnPIIInfo columnResult = pipelineService.analyzeColumn(
                            connectionId, dbType, tableName, columnName, samples);

                    // Set column type from metadata
                    columnResult.setColumnType(column.getType());

                    // Ensure additionalInfo is not null
                    if (columnResult.getAdditionalInfo() == null) {
                        columnResult.setAdditionalInfo(new HashMap<>());
                    }

                    // Clean results to eliminate redundant detections
                    columnResult = resultCleaner.cleanDetections(columnResult);

                    tableResult.addColumnResult(columnResult);
                } catch (Exception e) {
                    log.error("Error while analyzing column {}.{}: {}", tableName, column.getName(), e.getMessage(), e);

                    // Add error result to not lose the column
                    ColumnPIIInfo errorResult = ColumnPIIInfo.builder()
                            .columnName(column.getName())
                            .tableName(tableName)
                            .columnType(column.getType())
                            .piiDetected(false)
                            .detections(new ArrayList<>())
                            .additionalInfo(new HashMap<>())
                            .build();

                    errorResult.getAdditionalInfo().put("error", e.getMessage());
                    tableResult.addColumnResult(errorResult);
                }
            }

            // Force garbage collection between batches for large tables
            if (columns.size() > 50 && columnBatches.size() > 2) {
                System.gc();
            }
        }

        log.info("Analysis of table {} completed. Columns containing PII: {}/{}",
                tableName,
                tableResult.getColumnResults().stream().filter(ColumnPIIInfo::isPiiDetected).count(),
                tableResult.getColumnResults().size());

        long tableEndTime = System.currentTimeMillis();
        long tableProcessingTime = tableEndTime - tableStartTime;
        metricsCollector.recordTableProcessingTime(tableName, tableProcessingTime);

        // Cache the result
        tableResultCache.put(cacheKey, tableResult);

        return tableResult;
    }

    @Override
    public ColumnPIIInfo detectPIIInColumn(String connectionId, String dbType, String tableName, String columnName) {
        log.debug("Analyzing column: {}.{}", tableName, columnName);

        // Get column metadata
        List<ColumnMetadata> columns = scannerService.scanColumns(dbType, connectionId, tableName);
        Optional<ColumnMetadata> columnMetaOpt = columns.stream()
                .filter(col -> col.getName().equals(columnName))
                .findFirst();

        if (!columnMetaOpt.isPresent()) {
            log.warn("Column {} not found in table {}", columnName, tableName);
            return ColumnPIIInfo.builder()
                    .columnName(columnName)
                    .tableName(tableName)
                    .piiDetected(false)
                    .detections(new ArrayList<>())
                    .additionalInfo(new HashMap<>())
                    .build();
        }

        ColumnMetadata columnMeta = columnMetaOpt.get();

        // Get the appropriate sample size for this table
        int effectiveSampleSize = tableSampleSizes.getOrDefault(tableName, sampleSize);

        // Sample data
        List<Object> sampleData = null;
        try {
            sampleData = samplingService.sampleColumn(dbType, connectionId, tableName, columnName, effectiveSampleSize);
        } catch (Exception e) {
            log.warn("Unable to sample column {}.{}: {}", tableName, columnName, e.getMessage());
            // Continue with empty sample
            sampleData = Collections.emptyList();
        }

        // Use the detection pipeline to apply strategies sequentially
        ColumnPIIInfo columnResult = pipelineService.analyzeColumn(
                connectionId, dbType, tableName, columnName, sampleData);

        // Set column type from metadata
        columnResult.setColumnType(columnMeta.getType());

        // Ensure additionalInfo is not null
        if (columnResult.getAdditionalInfo() == null) {
            columnResult.setAdditionalInfo(new HashMap<>());
        }

        log.debug("Analysis of column {}.{} completed. PII detected: {}, most likely type: {}",
                tableName, columnName, columnResult.isPiiDetected(),
                columnResult.isPiiDetected() ? columnResult.getMostLikelyPIIType() : "UNKNOWN");

        return columnResult;
    }

    @Override
    public void setConfidenceThreshold(double confidenceThreshold) {
        if (confidenceThreshold < 0.0 || confidenceThreshold > 1.0) {
            throw new IllegalArgumentException("Confidence threshold must be between 0.0 and 1.0");
        }
        this.confidenceThreshold = confidenceThreshold;

        // Propagate threshold to pipeline
        pipelineService.setConfidenceThreshold(confidenceThreshold);

        // Also propagate to individual strategies for consistency
        strategyFactory.getAllStrategies().forEach(strategy -> {
            strategy.setConfidenceThreshold(confidenceThreshold);
        });
    }

    @Override
    public void configureStrategies(Map<String, Boolean> strategyMap) {
        this.activeStrategies.putAll(strategyMap);
    }

    /**
     * Batch samples multiple columns from a table for efficiency.
     *
     * @param connectionId Connection identifier
     * @param dbType Database type
     * @param tableName Table name
     * @param columnNames List of column names to sample
     * @param sampleSize Number of samples to fetch per column
     * @return Map of column names to sample data
     */
    private Map<String, List<Object>> batchSampleColumns(String connectionId, String dbType,
                                                         String tableName, List<String> columnNames,
                                                         int sampleSize) {
        try {
            // This could be implemented using a bulk query if the underlying database supports it
            // For now, we'll use a parallel approach

            Map<String, List<Object>> results = new ConcurrentHashMap<>();

            // Use parallel streams for faster sampling with many columns
            if (columnNames.size() > 10) {
                columnNames.parallelStream().forEach(columnName -> {
                    try {
                        List<Object> samples = samplingService.sampleColumn(
                                dbType, connectionId, tableName, columnName, sampleSize);
                        results.put(columnName, samples);
                    } catch (Exception e) {
                        log.warn("Error sampling column {}.{}: {}", tableName, columnName, e.getMessage());
                        // Continue with next column
                    }
                });
            } else {
                // Use sequential processing for smaller sets to reduce overhead
                for (String columnName : columnNames) {
                    try {
                        List<Object> samples = samplingService.sampleColumn(
                                dbType, connectionId, tableName, columnName, sampleSize);
                        results.put(columnName, samples);
                    } catch (Exception e) {
                        log.warn("Error sampling column {}.{}: {}", tableName, columnName, e.getMessage());
                        // Continue with next column
                    }
                }
            }

            return results;
        } catch (Exception e) {
            log.error("Error during batch sampling of table {}: {}", tableName, e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    /**
     * Partitions a list into smaller sublists of specified size.
     *
     * @param <T> List element type
     * @param list Original list to partition
     * @param size Maximum size of each sublist
     * @return List of sublists
     */
    private <T> List<List<T>> partitionList(List<T> list, int size) {
        if (list == null || list.isEmpty() || size <= 0) {
            return Collections.emptyList();
        }

        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }

        return partitions;
    }

    /**
     * Configures adaptive sample sizes for tables based on their size.
     * Uses larger samples for small tables and smaller samples for large tables.
     *
     * @param connectionId Connection identifier
     * @param dbType Database type
     * @param tables List of tables
     */
    private void configureAdaptiveSampleSizes(String connectionId, String dbType, List<TableMetadata> tables) {
        for (TableMetadata table : tables) {
            try {
                int columnCount = scannerService.scanColumns(dbType, connectionId, table.getName()).size();

                // Estimate row count (could be improved with actual row counts)
                // Using column count as a simple proxy for table size
                int estimatedRowCount = 1000;  // Default assumption

                // Adjust sample size based on estimated row count
                int adaptiveSampleSize;

                if (columnCount <= 5) {
                    // Small tables: use larger samples (up to 20)
                    adaptiveSampleSize = Math.min(20, sampleSize * 2);
                } else if (columnCount >= 50) {
                    // Very large tables: use minimum samples
                    adaptiveSampleSize = Math.max(5, sampleSize / 2);
                } else {
                    // Medium tables: use standard sample size
                    adaptiveSampleSize = sampleSize;
                }

                tableSampleSizes.put(table.getName(), adaptiveSampleSize);

                log.debug("Configured adaptive sample size for table {}: {} columns, {} samples",
                        table.getName(), columnCount, adaptiveSampleSize);

            } catch (Exception e) {
                log.warn("Error configuring adaptive sample size for table {}: {}",
                        table.getName(), e.getMessage());
                // Fallback to default sample size
                tableSampleSizes.put(table.getName(), sampleSize);
            }
        }
    }

    /**
     * TokenBucket implementation for rate limiting.
     * Used to prevent overwhelming external services like NER.
     */
    private static class TokenBucket {
        private final long refillInterval;
        private final TimeUnit timeUnit;
        private final int capacity;
        private final AtomicInteger availableTokens;
        private final long lastRefillTimestamp;

        public TokenBucket(int capacity, long refillInterval, TimeUnit timeUnit) {
            this.capacity = capacity;
            this.refillInterval = refillInterval;
            this.timeUnit = timeUnit;
            this.availableTokens = new AtomicInteger(capacity);
            this.lastRefillTimestamp = System.currentTimeMillis();
        }

        public boolean tryConsume() {
            refillIfNeeded();
            return availableTokens.getAndUpdate(current -> current > 0 ? current - 1 : 0) > 0;
        }

        private void refillIfNeeded() {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastRefillTimestamp;
            long refillIntervalMs = timeUnit.toMillis(refillInterval);

            if (elapsedTime >= refillIntervalMs) {
                availableTokens.set(capacity);
            }
        }
    }

    /**
     * Sets the sample size to use for analysis.
     *
     * @param sampleSize Number of records to sample per column
     */
    public void setSampleSize(int sampleSize) {
        if (sampleSize < 1) {
            throw new IllegalArgumentException("Sample size must be positive");
        }
        this.sampleSize = sampleSize;
        // Reset table-specific sample sizes
        tableSampleSizes.clear();
    }

    /**
     * Sets an adaptive sample size for a specific table.
     *
     * @param tableName Table name
     * @param rowCount Approximate row count for sizing calculation
     */
    public void setSampleSizeAdaptive(String tableName, int rowCount) {
        // Tables with fewer rows: bigger percentage sample
        // Tables with more rows: smaller percentage but minimum size guaranteed
        int adaptiveSize = Math.min(
                Math.max(5, rowCount / 1000), // Minimum 5, maximum 1/1000th of rows
                50 // Absolute maximum
        );
        tableSampleSizes.put(tableName, adaptiveSize);
    }

    /**
     * Clears all caches in the PII detector.
     * Should be called when refreshing analysis with new configuration.
     */
    public void clearCaches() {
        tableResultCache.clear();
        if (pipelineService instanceof PIIDetectionPipelineService) {
            ((PIIDetectionPipelineService) pipelineService).clearCache();
        }
        if (strategyFactory instanceof PIIDetectionStrategyFactoryImpl) {
            ((PIIDetectionStrategyFactoryImpl) strategyFactory).clearCompositeCache();
        }
        log.info("All PII detector caches cleared");
    }
}