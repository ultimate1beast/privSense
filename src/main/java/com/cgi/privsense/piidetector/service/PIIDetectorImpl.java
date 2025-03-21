/*
 * PIIDetectorImpl.java - Main implementation of the PII detector
 */
package com.cgi.privsense.piidetector.service;

import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;
import com.cgi.privsense.dbscanner.service.OptimizedParallelSamplingService;
import com.cgi.privsense.dbscanner.service.ScannerService;

import com.cgi.privsense.piidetector.api.PIIDetectionStrategy;
import com.cgi.privsense.piidetector.api.PIIDetectionStrategyFactory;
import com.cgi.privsense.piidetector.api.PIIDetector;
import com.cgi.privsense.piidetector.api.PIIReportGenerator;
import com.cgi.privsense.piidetector.model.*;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;


import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Main implementation of the PII detector.
 * Uses a pipeline approach to sequentially apply detection strategies.
 * Optimized to use the producer-consumer pattern for data sampling.
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

    @Autowired
    public PIIDetectorImpl(
            ScannerService scannerService,
            OptimizedParallelSamplingService samplingService,
            PIIDetectionStrategyFactory strategyFactory,
            PIIReportGenerator reportGenerator,
            PIIDetectionPipelineService pipelineService,
            PIIDetectionMetricsCollector metricsCollector,
            DetectionResultCleaner resultCleaner,
            @Value("${piidetector.confidence.threshold:0.7}") double confidenceThreshold,
            @Value("${piidetector.sampling.limit:10}") int sampleSize) {

        this.scannerService = scannerService;
        this.samplingService = samplingService;
        this.strategyFactory = strategyFactory;
        this.reportGenerator = reportGenerator;
        this.pipelineService = pipelineService;
        this.metricsCollector = metricsCollector;
        this.resultCleaner = resultCleaner;
        this.confidenceThreshold = confidenceThreshold;
        this.sampleSize = sampleSize;

        // Default: enable all strategies
        strategyFactory.getAllStrategies().forEach(strategy -> {
            activeStrategies.put(strategy.getName(), true);
        });

        log.info("PII Detector initialized");
    }

    @PostConstruct
    public void validateConfiguration() {
        if (confidenceThreshold < 0.0 || confidenceThreshold > 1.0) {
            throw new IllegalStateException("Confidence threshold must be between 0.0 and 1.0");
        }

        if (sampleSize <= 0) {
            throw new IllegalStateException("Sample size must be positive");
        }

        log.info("PIIDetector configuration validated: threshold={}, sampleSize={}",
                confidenceThreshold, sampleSize);
    }

    @Override
    public PIIDetectionResult detectPII(String connectionId, String dbType) {
        // Reset state and prepare
        resetState();
        long startTime = System.currentTimeMillis();

        log.info("Starting PII detection for connection: {}, type: {}", connectionId, dbType);

        // Initialize the result
        PIIDetectionResult result = initializeResult(connectionId, dbType);

        // Get and sort tables by size
        List<TableMetadata> tables = getAndSortTables(connectionId, dbType);

        // Process each table
        for (TableMetadata table : tables) {
            processTable(result, connectionId, dbType, table);
        }

        // Finalize the result
        finalizeResult(result, startTime);

        return result;
    }

    @Override
    @Cacheable(value = "tableResults", key = "#connectionId + ':' + #dbType + ':' + #tableName")
    public TablePIIInfo detectPIIInTable(String connectionId, String dbType, String tableName) {
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
        Map<String, List<Object>> columnSamples = fetchColumnSamples(connectionId, dbType, tableName, columns, effectiveSampleSize);

        // Process columns in batch sizes to limit memory usage
        List<List<ColumnMetadata>> columnBatches = partitionList(columns, 20);

        for (List<ColumnMetadata> batch : columnBatches) {
            for (ColumnMetadata column : batch) {
                String columnName = column.getName();

                // Get pre-fetched samples or fetch individually if not available
                List<Object> samples = columnSamples.getOrDefault(columnName, null);
                if (samples == null) {
                    samples = fetchIndividualColumnSample(dbType, connectionId, tableName, columnName, effectiveSampleSize);
                }

                // Process the column
                ColumnPIIInfo columnResult = processColumn(connectionId, dbType, tableName, column, samples);
                tableResult.addColumnResult(columnResult);
            }
        }

        log.info("Analysis of table {} completed. Columns containing PII: {}/{}",
                tableName,
                tableResult.getColumnResults().stream().filter(ColumnPIIInfo::isPiiDetected).count(),
                tableResult.getColumnResults().size());

        long tableEndTime = System.currentTimeMillis();
        long tableProcessingTime = tableEndTime - tableStartTime;
        metricsCollector.recordTableProcessingTime(tableName, tableProcessingTime);

        return tableResult;
    }

    @Override
    public ColumnPIIInfo detectPIIInColumn(String connectionId, String dbType, String tableName, String columnName) {
        log.debug("Analyzing column: {}.{}", tableName, columnName);

        // Get column metadata
        ColumnMetadata columnMeta = getColumnMetadata(connectionId, dbType, tableName, columnName);
        if (columnMeta == null) {
            return createEmptyColumnResult(tableName, columnName);
        }

        // Get the appropriate sample size for this table
        int effectiveSampleSize = tableSampleSizes.getOrDefault(tableName, sampleSize);

        // Sample data
        List<Object> sampleData = fetchIndividualColumnSample(dbType, connectionId, tableName, columnName, effectiveSampleSize);

        // Process the column
        return processColumn(connectionId, dbType, tableName, columnMeta, sampleData);
    }

    @Override
    public void setConfidenceThreshold(double confidenceThreshold) {
        if (confidenceThreshold < 0.0 || confidenceThreshold > 1.0) {
            throw new IllegalArgumentException("Confidence threshold must be between 0.0 and 1.0");
        }
        this.confidenceThreshold = confidenceThreshold;

        // Propagate threshold changes
        pipelineService.setConfidenceThreshold(confidenceThreshold);
        for (PIIDetectionStrategy strategy : strategyFactory.getAllStrategies()) {
            strategy.setConfidenceThreshold(confidenceThreshold);
        }

        // Clear caches
        clearCaches();
    }

    @Override
    public void configureStrategies(Map<String, Boolean> strategyMap) {
        this.activeStrategies.putAll(strategyMap);
        clearCaches();
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
        int adaptiveSize = Math.min(
                Math.max(5, rowCount / 1000), // Minimum 5, maximum 1/1000th of rows
                50 // Absolute maximum
        );
        tableSampleSizes.put(tableName, adaptiveSize);
    }

    /**
     * Clears all caches in the PII detector.
     */
    @CacheEvict(value = "tableResults", allEntries = true)
    public void clearCaches() {
        if (pipelineService instanceof PIIDetectionPipelineService) {
            ((PIIDetectionPipelineService) pipelineService).clearCache();
        }
        if (strategyFactory instanceof PIIDetectionStrategyFactoryImpl) {
            ((PIIDetectionStrategyFactoryImpl) strategyFactory).clearCompositeCache();
        }
        log.info("All PII detector caches cleared");
    }

    /* Private helper methods */

    private void resetState() {
        metricsCollector.resetMetrics();
        clearCaches();
    }

    private PIIDetectionResult initializeResult(String connectionId, String dbType) {
        return PIIDetectionResult.builder()
                .connectionId(connectionId)
                .dbType(dbType)
                .tableResults(new ArrayList<>())
                .piiTypeCounts(new HashMap<>())
                .additionalMetadata(new HashMap<>())
                .build();
    }

    private List<TableMetadata> getAndSortTables(String connectionId, String dbType) {
        List<TableMetadata> tables = scannerService.scanTables(dbType, connectionId);
        log.info("Number of tables found: {}", tables.size());

        // Configure adaptive sample sizes
        configureAdaptiveSampleSizes(connectionId, dbType, tables);

        // Sort tables by size
        List<TableMetadata> sortedTables = new ArrayList<>(tables);
        sortedTables.sort(Comparator.comparingInt(table -> {
            try {
                return scannerService.scanColumns(dbType, connectionId, table.getName()).size();
            } catch (Exception e) {
                return Integer.MAX_VALUE;
            }
        }));

        return sortedTables;
    }

    private void processTable(PIIDetectionResult result, String connectionId, String dbType, TableMetadata table) {
        try {
            TablePIIInfo tableResult = detectPIIInTable(connectionId, dbType, table.getName());
            result.addTableResult(tableResult);
            log.info("Processed table: {}, PII detected: {}", table.getName(), tableResult.isHasPii());
        } catch (Exception e) {
            log.error("Error processing table {}: {}", table.getName(), e.getMessage(), e);
        }
    }

    private void finalizeResult(PIIDetectionResult result, long startTime) {
        long endTime = System.currentTimeMillis();
        result.setProcessingTimeMs(endTime - startTime);

        log.info("PII detection completed. Total time: {} ms. PIIs detected: {}",
                result.getProcessingTimeMs(), result.getTotalPiiCount());

        // Add metrics
        result.getAdditionalMetadata().put("performanceMetrics", metricsCollector.getMetricsReport());

        // Log metrics
        metricsCollector.logMetricsReport();
    }

    private Map<String, List<Object>> fetchColumnSamples(String connectionId, String dbType, String tableName,
                                                         List<ColumnMetadata> columns, int sampleSize) {
        try {
            List<String> columnNames = columns.stream()
                    .map(ColumnMetadata::getName)
                    .collect(Collectors.toList());

            Map<String, List<Object>> samples = samplingService.sampleColumnsInParallel(
                    dbType, connectionId, tableName, columnNames, sampleSize);

            log.debug("Sampled {} columns from table {}", samples.size(), tableName);
            return samples;
        } catch (Exception e) {
            log.warn("Error while batch sampling table {}: {}", tableName, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private List<Object> fetchIndividualColumnSample(String dbType, String connectionId, String tableName,
                                                     String columnName, int sampleSize) {
        try {
            return samplingService.sampleColumn(dbType, connectionId, tableName, columnName, sampleSize);
        } catch (Exception e) {
            log.warn("Error sampling column {}.{}: {}", tableName, columnName, e.getMessage());
            return Collections.emptyList();
        }
    }

    private ColumnPIIInfo processColumn(String connectionId, String dbType, String tableName,
                                        ColumnMetadata column, List<Object> samples) {
        String columnName = column.getName();

        try {
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
            return resultCleaner.cleanDetections(columnResult);
        } catch (Exception e) {
            log.error("Error while analyzing column {}.{}: {}", tableName, columnName, e.getMessage(), e);

            // Return error result
            ColumnPIIInfo errorResult = ColumnPIIInfo.builder()
                    .columnName(columnName)
                    .tableName(tableName)
                    .columnType(column.getType())
                    .piiDetected(false)
                    .detections(new ArrayList<>())
                    .additionalInfo(new HashMap<>())
                    .build();

            errorResult.getAdditionalInfo().put("error", e.getMessage());
            return errorResult;
        }
    }

    private ColumnMetadata getColumnMetadata(String connectionId, String dbType, String tableName, String columnName) {
        try {
            List<ColumnMetadata> columns = scannerService.scanColumns(dbType, connectionId, tableName);
            return columns.stream()
                    .filter(col -> col.getName().equals(columnName))
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            log.warn("Error getting column metadata for {}.{}: {}", tableName, columnName, e.getMessage());
            return null;
        }
    }

    private ColumnPIIInfo createEmptyColumnResult(String tableName, String columnName) {
        return ColumnPIIInfo.builder()
                .columnName(columnName)
                .tableName(tableName)
                .piiDetected(false)
                .detections(new ArrayList<>())
                .additionalInfo(new HashMap<>())
                .build();
    }

    protected void configureAdaptiveSampleSizes(String connectionId, String dbType, List<TableMetadata> tables) {
        for (TableMetadata table : tables) {
            try {
                int columnCount = scannerService.scanColumns(dbType, connectionId, table.getName()).size();

                // Adjust sample size based on column count
                int adaptiveSampleSize;

                if (columnCount <= 5) {
                    // Small tables: use larger samples
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

    protected <T> List<List<T>> partitionList(List<T> list, int size) {
        if (list == null || list.isEmpty() || size <= 0) {
            return Collections.emptyList();
        }

        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }

        return partitions;
    }
}