package com.cgi.privsense.piidetector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Instrumentation service for collecting performance metrics
 * during PII detection.
 */
@Component
public class PIIDetectionMetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(PIIDetectionMetricsCollector.class);

    // Counters for different detection methods
    private final AtomicInteger heuristicDetectionCount = new AtomicInteger(0);
    private final AtomicInteger regexDetectionCount = new AtomicInteger(0);
    private final AtomicInteger nerDetectionCount = new AtomicInteger(0);

    // Counters for columns stopped at each pipeline stage
    private final AtomicInteger heuristicStopCount = new AtomicInteger(0);
    private final AtomicInteger regexStopCount = new AtomicInteger(0);
    private final AtomicInteger nerStopCount = new AtomicInteger(0);

    // Total execution time for each method
    private final Map<String, Long> detectionTimes = new ConcurrentHashMap<>();

    // Table-specific processing times
    private final Map<String, AtomicLong> tableProcessingTimes = new ConcurrentHashMap<>();

    // Column-specific processing times
    private final Map<String, AtomicLong> columnProcessingTimes = new ConcurrentHashMap<>();

    // Statistics by PII type detected
    private final Map<String, AtomicInteger> piiTypeStats = new ConcurrentHashMap<>();

    // Global metrics
    private long totalDetectionTimeMs = 0;
    private int totalColumnsProcessed = 0;
    private int totalPiiDetected = 0;

    // Skipped columns counter
    private final AtomicInteger skippedColumnsCount = new AtomicInteger(0);

    /**
     * Records a heuristic detection.
     */
    public void recordHeuristicDetection() {
        heuristicDetectionCount.incrementAndGet();
    }

    /**
     * Records a regex detection.
     */
    public void recordRegexDetection() {
        regexDetectionCount.incrementAndGet();
    }

    /**
     * Records a NER detection.
     */
    public void recordNerDetection() {
        nerDetectionCount.incrementAndGet();
    }

    /**
     * Records a pipeline stop at the heuristic stage.
     */
    public void recordHeuristicPipelineStop() {
        heuristicStopCount.incrementAndGet();
    }

    /**
     * Records a pipeline stop at the regex stage.
     */
    public void recordRegexPipelineStop() {
        regexStopCount.incrementAndGet();
    }

    /**
     * Records a pipeline stop at the NER stage.
     */
    public void recordNerPipelineStop() {
        nerStopCount.incrementAndGet();
    }

    /**
     * Records a skipped column.
     */
    public void recordSkippedColumn() {
        skippedColumnsCount.incrementAndGet();
    }

    /**
     * Records the execution time of a detection method.
     *
     * @param method Method name
     * @param timeMs Execution time in ms
     */
    public void recordDetectionTime(String method, long timeMs) {
        detectionTimes.compute(method, (k, v) -> (v == null) ? timeMs : v + timeMs);
    }

    /**
     * Records the processing time for a table.
     *
     * @param tableName Table name
     * @param timeMs Processing time in ms
     */
    public void recordTableProcessingTime(String tableName, long timeMs) {
        tableProcessingTimes.computeIfAbsent(tableName, k -> new AtomicLong(0))
                .addAndGet(timeMs);
    }

    /**
     * Records the processing time for a column.
     *
     * @param tableName Table name
     * @param columnName Column name
     * @param timeMs Processing time in ms
     */
    public void recordColumnProcessingTime(String tableName, String columnName, long timeMs) {
        String key = tableName + "." + columnName;
        columnProcessingTimes.computeIfAbsent(key, k -> new AtomicLong(0))
                .addAndGet(timeMs);
    }

    /**
     * Records a PII type detection.
     *
     * @param piiType Detected PII type
     */
    public void recordPiiTypeDetection(String piiType) {
        piiTypeStats.computeIfAbsent(piiType, k -> new AtomicInteger(0)).incrementAndGet();
        totalPiiDetected++;
    }

    /**
     * Records a processed column with its total time.
     *
     * @param timeMs Total column processing time
     */
    public void recordColumnProcessed(long timeMs) {
        totalDetectionTimeMs += timeMs;
        totalColumnsProcessed++;
    }

    /**
     * Resets all metrics.
     */
    public void resetMetrics() {
        heuristicDetectionCount.set(0);
        regexDetectionCount.set(0);
        nerDetectionCount.set(0);
        heuristicStopCount.set(0);
        regexStopCount.set(0);
        nerStopCount.set(0);
        skippedColumnsCount.set(0);
        detectionTimes.clear();
        tableProcessingTimes.clear();
        columnProcessingTimes.clear();
        piiTypeStats.clear();
        totalDetectionTimeMs = 0;
        totalColumnsProcessed = 0;
        totalPiiDetected = 0;
    }

    /**
     * Generates a report of collected metrics.
     *
     * @return Map containing all metrics
     */
    public Map<String, Object> getMetricsReport() {
        Map<String, Object> report = new HashMap<>();

        // Detection statistics by method
        report.put("heuristicDetections", heuristicDetectionCount.get());
        report.put("regexDetections", regexDetectionCount.get());
        report.put("nerDetections", nerDetectionCount.get());

        // Pipeline stop statistics
        report.put("heuristicStops", heuristicStopCount.get());
        report.put("regexStops", regexStopCount.get());
        report.put("nerStops", nerStopCount.get());
        report.put("skippedColumns", skippedColumnsCount.get());

        // Pipeline stop percentages
        int totalColumns = totalColumnsProcessed > 0 ? totalColumnsProcessed : 1; // Avoid division by zero
        report.put("heuristicStopPercentage", (double) heuristicStopCount.get() / totalColumns * 100);
        report.put("regexStopPercentage", (double) regexStopCount.get() / totalColumns * 100);
        report.put("nerStopPercentage", (double) nerStopCount.get() / totalColumns * 100);
        report.put("skippedColumnsPercentage", (double) skippedColumnsCount.get() /
                (totalColumns + skippedColumnsCount.get()) * 100);

        // Execution times
        report.put("detectionTimes", detectionTimes);
        report.put("totalDetectionTimeMs", totalDetectionTimeMs);
        report.put("averageColumnTimeMs", totalColumnsProcessed > 0 ?
                (double) totalDetectionTimeMs / totalColumnsProcessed : 0);

        // Add table processing times (top 10 by time)
        Map<String, Long> top10Tables = new HashMap<>();
        tableProcessingTimes.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get())) // Reverse order (highest first)
                .limit(10)
                .forEach(entry -> top10Tables.put(entry.getKey(), entry.getValue().get()));
        report.put("top10TablesByTime", top10Tables);

        // Add column processing times (top 10 by time)
        Map<String, Long> top10Columns = new HashMap<>();
        columnProcessingTimes.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get())) // Reverse order
                .limit(10)
                .forEach(entry -> top10Columns.put(entry.getKey(), entry.getValue().get()));
        report.put("top10ColumnsByTime", top10Columns);

        // Statistics by PII type
        Map<String, Integer> piiTypeCounts = new HashMap<>();
        piiTypeStats.forEach((key, value) -> piiTypeCounts.put(key, value.get()));
        report.put("piiTypeStats", piiTypeCounts);

        // Global metrics
        report.put("totalColumnsProcessed", totalColumnsProcessed);
        report.put("totalPiiDetected", totalPiiDetected);

        return report;
    }

    /**
     * Logs a metrics report.
     */
    public void logMetricsReport() {
        Map<String, Object> report = getMetricsReport();

        log.info("=== PII Detection Pipeline Performance Report ===");
        log.info("Columns processed: {}", report.get("totalColumnsProcessed"));
        log.info("Columns skipped: {}", report.get("skippedColumns"));
        log.info("PIIs detected: {}", report.get("totalPiiDetected"));
        log.info("Total time: {} ms", report.get("totalDetectionTimeMs"));
        log.info("Average time per column: {} ms", report.get("averageColumnTimeMs"));

        log.info("--- Pipeline Stop Statistics ---");
        log.info("Stops at heuristic stage: {} ({}%)",
                report.get("heuristicStops"), String.format("%.2f", report.get("heuristicStopPercentage")));
        log.info("Stops at regex stage: {} ({}%)",
                report.get("regexStops"), String.format("%.2f", report.get("regexStopPercentage")));
        log.info("Stops at NER stage: {} ({}%)",
                report.get("nerStops"), String.format("%.2f", report.get("nerStopPercentage")));

        log.info("--- Detections by Method ---");
        log.info("Heuristic detections: {}", report.get("heuristicDetections"));
        log.info("Regex detections: {}", report.get("regexDetections"));
        log.info("NER detections: {}", report.get("nerDetections"));

        log.info("--- Execution Time by Method ---");
        @SuppressWarnings("unchecked")
        Map<String, Long> times = (Map<String, Long>) report.get("detectionTimes");
        times.forEach((method, timeMs) -> log.info("{}: {} ms", method, timeMs));

        log.info("--- Detections by PII Type ---");
        @SuppressWarnings("unchecked")
        Map<String, Integer> piiCounts = (Map<String, Integer>) report.get("piiTypeStats");
        piiCounts.forEach((type, count) -> log.info("{}: {}", type, count));

        log.info("--- Top 10 Tables by Processing Time ---");
        @SuppressWarnings("unchecked")
        Map<String, Long> top10Tables = (Map<String, Long>) report.get("top10TablesByTime");
        top10Tables.forEach((table, timeMs) -> log.info("{}: {} ms", table, timeMs));
    }
}