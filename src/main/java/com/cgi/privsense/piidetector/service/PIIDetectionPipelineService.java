package com.cgi.privsense.piidetector.service;

import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.PIITypeDetection;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import com.cgi.privsense.piidetector.strategy.HeuristicNameStrategy;
import com.cgi.privsense.piidetector.strategy.NERModelStrategy;
import com.cgi.privsense.piidetector.strategy.RegexPatternStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Service that implements a pipeline approach for PII detection.
 * Detectors are applied sequentially, stopping as soon as a detection
 * with sufficient confidence is found.
 */
@Service
public class PIIDetectionPipelineService {
    private static final Logger log = LoggerFactory.getLogger(PIIDetectionPipelineService.class);

    private final HeuristicNameStrategy heuristicStrategy;
    private final RegexPatternStrategy regexStrategy;
    private final NERModelStrategy nerStrategy;
    private final PIIDetectionMetricsCollector metricsCollector;

    private double confidenceThreshold = 0.7;

    // Common technical columns that can be safely skipped
    private static final Set<String> SKIP_COLUMNS = new HashSet<>(Arrays.asList(
            "id", "created_at", "updated_at", "created_by", "modified_at", "rowid", "uuid"
    ));

    // Pattern to identify technical columns
    private static final Pattern TECHNICAL_COLUMN_PATTERN = Pattern.compile(
            "^(id|_id|.*_id|pk|fk|.*_pk|.*_fk|seq|sequence|timestamp|version|active|is_.*|has_.*|flag)$",
            Pattern.CASE_INSENSITIVE
    );

    // Cache for column analysis results to avoid redundant processing
    private final Map<String, ColumnPIIInfo> columnResultCache = new ConcurrentHashMap<>();

    public PIIDetectionPipelineService(
            HeuristicNameStrategy heuristicStrategy,
            RegexPatternStrategy regexStrategy,
            NERModelStrategy nerStrategy,
            PIIDetectionMetricsCollector metricsCollector,
            @Value("${piidetector.confidence.threshold}") double confidenceThreshold) {
        this.heuristicStrategy = heuristicStrategy;
        this.regexStrategy = regexStrategy;
        this.nerStrategy = nerStrategy;
        this.metricsCollector = metricsCollector;
        this.confidenceThreshold = confidenceThreshold;

        // Propagate confidence threshold to all strategies
        heuristicStrategy.setConfidenceThreshold(confidenceThreshold);
        regexStrategy.setConfidenceThreshold(confidenceThreshold);
        nerStrategy.setConfidenceThreshold(confidenceThreshold);

        log.info("PII detection pipeline initialized with confidence threshold: {}", confidenceThreshold);
    }

    /**
     * Analyzes a column by applying strategies sequentially.
     * Stops as soon as a detection method gives a result with sufficient confidence.
     *
     * @param connectionId Connection identifier
     * @param dbType Database type
     * @param tableName Table name
     * @param columnName Column name
     * @param sampleData Data samples for this column
     * @return Information about PIIs detected in the column
     */
    public ColumnPIIInfo analyzeColumn(String connectionId, String dbType, String tableName,
                                       String columnName, List<Object> sampleData) {
        log.debug("Starting detection pipeline for column {}.{}", tableName, columnName);

        // Check cache first
        String cacheKey = connectionId + ":" + dbType + ":" + tableName + ":" + columnName;
        if (columnResultCache.containsKey(cacheKey)) {
            log.debug("Using cached result for {}.{}", tableName, columnName);
            return columnResultCache.get(cacheKey);
        }

        // Preliminary check to skip known technical columns
        if (shouldSkipColumn(columnName)) {
            log.debug("Skipping technical column {}.{}", tableName, columnName);
            metricsCollector.recordSkippedColumn();
            ColumnPIIInfo emptyResult = createEmptyResult(tableName, columnName);
            columnResultCache.put(cacheKey, emptyResult);
            return emptyResult;
        }

        long startTime = System.currentTimeMillis();

        ColumnPIIInfo columnInfo = ColumnPIIInfo.builder()
                .columnName(columnName)
                .tableName(tableName)
                .piiDetected(false)
                .detections(new ArrayList<>())
                .build();

        // Stage 1: Heuristic detection based on column name (fast, no data access)
        if (heuristicStrategy.isApplicable(true, false)) {
            long heuristicStart = System.currentTimeMillis();
            try {
                ColumnPIIInfo heuristicResult = heuristicStrategy.detectColumnPII(
                        connectionId, dbType, tableName, columnName, null);

                if (heuristicResult.isPiiDetected()) {
                    // Check if a detection has sufficient confidence
                    Optional<PIITypeDetection> highConfidenceDetection = getHighConfidenceDetection(heuristicResult);

                    // If we have a high confidence detection, stop here
                    if (highConfidenceDetection.isPresent()) {
                        log.debug("Heuristic detection sufficient for {}.{}, confidence={}",
                                tableName, columnName, highConfidenceDetection.get().getConfidence());

                        columnInfo.setColumnType(heuristicResult.getColumnType());
                        columnInfo.getDetections().add(highConfidenceDetection.get());
                        columnInfo.setPiiDetected(true);

                        // Record metrics
                        metricsCollector.recordHeuristicDetection();
                        metricsCollector.recordHeuristicPipelineStop();
                        metricsCollector.recordPiiTypeDetection(highConfidenceDetection.get().getPiiType().name());

                        long totalTime = System.currentTimeMillis() - startTime;
                        metricsCollector.recordColumnProcessed(totalTime);
                        metricsCollector.recordColumnProcessingTime(tableName, columnName, totalTime);

                        // Cache the result
                        columnResultCache.put(cacheKey, columnInfo);

                        return columnInfo;
                    }

                    // Otherwise, add the best detection and continue
                    Optional<PIITypeDetection> bestDetection = heuristicResult.getDetections().stream()
                            .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()));

                    if (bestDetection.isPresent()) {
                        columnInfo.getDetections().add(bestDetection.get());
                        metricsCollector.recordHeuristicDetection();
                        metricsCollector.recordPiiTypeDetection(bestDetection.get().getPiiType().name());

                        // Enhanced detection with context (adjacent columns)
                        enhanceConfidenceWithContext(tableName, columnName, columnInfo);
                    }
                } else {
                    log.debug("No PII detected by heuristic strategy for {}.{}", tableName, columnName);
                }
            } catch (Exception e) {
                log.error("Error during heuristic detection on {}.{}: {}",
                        tableName, columnName, e.getMessage(), e);
            }
            long heuristicEnd = System.currentTimeMillis();
            metricsCollector.recordDetectionTime("heuristic", heuristicEnd - heuristicStart);
        }

        // If no samples available, we can't go further
        if (sampleData == null || sampleData.isEmpty()) {
            log.warn("No samples available for {}.{}, ending pipeline", tableName, columnName);
            long totalTime = System.currentTimeMillis() - startTime;
            metricsCollector.recordColumnProcessed(totalTime);
            metricsCollector.recordColumnProcessingTime(tableName, columnName, totalTime);

            // Cache the result
            columnResultCache.put(cacheKey, columnInfo);

            return columnInfo;
        }

        // Pre-filter samples to optimize regex processing
        List<Object> filteredSamples = preFilterSamples(sampleData);

        log.debug("Checking regex strategy applicability for {}.{}: {}",
                tableName, columnName, regexStrategy.isApplicable(true, filteredSamples != null && !filteredSamples.isEmpty()));

        // Stage 2: Regex detection (quite fast)
        if (regexStrategy.isApplicable(true, true) && !filteredSamples.isEmpty()) {
            long regexStart = System.currentTimeMillis();
            try {
                ColumnPIIInfo regexResult = regexStrategy.detectColumnPII(
                        connectionId, dbType, tableName, columnName, filteredSamples);

                log.debug("Regex strategy result for {}.{}: detected={}, detections count={}",
                        tableName, columnName, regexResult.isPiiDetected(),
                        regexResult.getDetections() != null ? regexResult.getDetections().size() : 0);

                if (regexResult.isPiiDetected()) {
                    // Check if a detection has sufficient confidence
                    Optional<PIITypeDetection> highConfidenceDetection = getHighConfidenceDetection(regexResult);

                    // If we have a high confidence detection, stop here
                    if (highConfidenceDetection.isPresent()) {
                        log.debug("Regex detection sufficient for {}.{}, confidence={}",
                                tableName, columnName, highConfidenceDetection.get().getConfidence());

                        columnInfo.setColumnType(regexResult.getColumnType());
                        columnInfo.getDetections().add(highConfidenceDetection.get());
                        columnInfo.setPiiDetected(true);

                        // Record metrics
                        metricsCollector.recordRegexDetection();
                        metricsCollector.recordRegexPipelineStop();
                        metricsCollector.recordPiiTypeDetection(highConfidenceDetection.get().getPiiType().name());

                        long totalTime = System.currentTimeMillis() - startTime;
                        metricsCollector.recordColumnProcessed(totalTime);
                        metricsCollector.recordColumnProcessingTime(tableName, columnName, totalTime);

                        // Cache the result
                        columnResultCache.put(cacheKey, columnInfo);

                        return columnInfo;
                    }

                    // Otherwise, add the best detection and continue
                    Optional<PIITypeDetection> bestDetection = regexResult.getDetections().stream()
                            .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()));

                    if (bestDetection.isPresent()) {
                        columnInfo.getDetections().add(bestDetection.get());
                        metricsCollector.recordRegexDetection();
                        metricsCollector.recordPiiTypeDetection(bestDetection.get().getPiiType().name());
                    }
                } else {
                    log.debug("No PII detected by regex strategy for {}.{}", tableName, columnName);
                }
            } catch (Exception e) {
                log.error("Error during regex detection on {}.{}: {}",
                        tableName, columnName, e.getMessage(), e);
            }
            long regexEnd = System.currentTimeMillis();
            metricsCollector.recordDetectionTime("regex", regexEnd - regexStart);
        }

        // If NER service is not available, stop here
        if (!nerStrategy.isApplicable(true, true) || !isNerServiceAvailable()) {
            // Set result based on detections so far
            columnInfo.setPiiDetected(!columnInfo.getDetections().isEmpty());

            long totalTime = System.currentTimeMillis() - startTime;
            metricsCollector.recordColumnProcessed(totalTime);
            metricsCollector.recordColumnProcessingTime(tableName, columnName, totalTime);

            // Cache the result
            columnResultCache.put(cacheKey, columnInfo);

            return columnInfo;
        }

        // Stage 3: NER detection (more expensive, applied as a last resort)
        {
            long nerStart = System.currentTimeMillis();
            try {
                ColumnPIIInfo nerResult = nerStrategy.detectColumnPII(
                        connectionId, dbType, tableName, columnName, sampleData);

                if (nerResult.isPiiDetected()) {
                    // Add all NER detections
                    columnInfo.setColumnType(nerResult.getColumnType());

                    // Record metrics for each detection
                    for (PIITypeDetection detection : nerResult.getDetections()) {
                        columnInfo.getDetections().add(detection);
                        metricsCollector.recordNerDetection();
                        metricsCollector.recordPiiTypeDetection(detection.getPiiType().name());
                    }

                    columnInfo.setPiiDetected(true);
                    metricsCollector.recordNerPipelineStop();
                } else {
                    log.debug("No PII detected by NER strategy for {}.{}", tableName, columnName);
                }
            } catch (Exception e) {
                log.error("Error during NER detection on {}.{}: {}",
                        tableName, columnName, e.getMessage(), e);
            }
            long nerEnd = System.currentTimeMillis();
            metricsCollector.recordDetectionTime("ner", nerEnd - nerStart);
        }

        // Determine if the column contains PII based on all detections
        columnInfo.setPiiDetected(!columnInfo.getDetections().isEmpty());

        log.debug("Detection pipeline completed for {}.{}, {} detections found",
                tableName, columnName, columnInfo.getDetections().size());

        long totalTime = System.currentTimeMillis() - startTime;
        metricsCollector.recordColumnProcessed(totalTime);
        metricsCollector.recordColumnProcessingTime(tableName, columnName, totalTime);

        // Cache the result
        columnResultCache.put(cacheKey, columnInfo);

        return columnInfo;
    }

    /**
     * Finds a detection with high confidence, if it exists.
     *
     * @param columnInfo Column information
     * @return High confidence detection (optional)
     */
    private Optional<PIITypeDetection> getHighConfidenceDetection(ColumnPIIInfo columnInfo) {
        if (columnInfo == null || !columnInfo.isPiiDetected()) {
            return Optional.empty();
        }

        // Consider 90% or higher confidence as "high"
        double highConfidenceThreshold = 0.9;

        return columnInfo.getDetections().stream()
                .filter(detection -> detection.getConfidence() >= highConfidenceThreshold)
                .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()));
    }

    /**
     * Checks if a column should be skipped based on naming patterns.
     *
     * @param columnName Column name to check
     * @return true if column should be skipped
     */
    private boolean shouldSkipColumn(String columnName) {
        if (columnName == null || columnName.isEmpty()) {
            return false;
        }

        String normalizedName = columnName.toLowerCase();

        // Check against known technical column names
        if (SKIP_COLUMNS.contains(normalizedName)) {
            return true;
        }

        // Check against technical column pattern
        return TECHNICAL_COLUMN_PATTERN.matcher(normalizedName).matches();
    }

    /**
     * Creates an empty result for a column.
     *
     * @param tableName Table name
     * @param columnName Column name
     * @return Empty column PII info
     */
    private ColumnPIIInfo createEmptyResult(String tableName, String columnName) {
        return ColumnPIIInfo.builder()
                .columnName(columnName)
                .tableName(tableName)
                .piiDetected(false)
                .detections(new ArrayList<>())
                .additionalInfo(new HashMap<>())
                .build();
    }

    /**
     * Pre-filters samples to optimize regex processing.
     * Removes null values and applies quick validation checks.
     *
     * @param samples Original samples
     * @return Filtered samples
     */
    private List<Object> preFilterSamples(List<Object> samples) {
        if (samples == null || samples.isEmpty()) {
            return Collections.emptyList();
        }

        List<Object> filteredSamples = new ArrayList<>(samples.size());
        for (Object sample : samples) {
            if (sample != null && sample.toString().length() > 0) {
                filteredSamples.add(sample);
            }
        }

        return filteredSamples;
    }

    /**
     * Enhances detection confidence based on context (adjacent columns).
     * For example, if first_name and last_name columns are adjacent, 
     * confidence for both can be increased.
     *
     * @param tableName Table name
     * @param columnName Column name
     * @param columnInfo Column information to enhance
     */
    private void enhanceConfidenceWithContext(String tableName, String columnName, ColumnPIIInfo columnInfo) {
        if (columnInfo == null || !columnInfo.isPiiDetected() || columnInfo.getDetections().isEmpty()) {
            return;
        }

        // Example: Enhance confidence for name-related fields if they appear in typical patterns
        String normalizedName = columnName.toLowerCase();

        if (normalizedName.contains("name") || normalizedName.contains("nom")) {
            // Check for related PII types in detections
            for (PIITypeDetection detection : columnInfo.getDetections()) {
                PIIType type = detection.getPiiType();

                if (type == PIIType.FIRST_NAME || type == PIIType.LAST_NAME || type == PIIType.FULL_NAME) {
                    // Enhance confidence, but cap at 0.95 to avoid overriding NER
                    double enhancedConfidence = Math.min(detection.getConfidence() * 1.2, 0.95);
                    detection.setConfidence(enhancedConfidence);

                    // Add enhancement information to metadata
                    detection.getDetectionMetadata().put("contextEnhanced", true);
                    detection.getDetectionMetadata().put("originalConfidence", detection.getConfidence());
                }
            }
        }
    }

    /**
     * Checks if the NER service is available.
     * Uses cached check to avoid frequent API calls.
     *
     * @return true if NER service is available
     */
    private boolean isNerServiceAvailable() {
        // This method would ideally check a cached status and only
        // try to connect to the NER service if the cache is stale
        return nerStrategy instanceof NERModelStrategy && ((NERModelStrategy) nerStrategy).isServiceAvailable();
    }

    /**
     * Updates the confidence threshold for all strategies.
     *
     * @param threshold New confidence threshold
     */
    public void setConfidenceThreshold(double threshold) {
        this.confidenceThreshold = threshold;

        // Propagate the change to all strategies
        heuristicStrategy.setConfidenceThreshold(threshold);
        regexStrategy.setConfidenceThreshold(threshold);
        nerStrategy.setConfidenceThreshold(threshold);

        // Clear the column result cache since threshold changed
        columnResultCache.clear();
    }

    /**
     * Clears the column result cache.
     * Should be called when configuration changes or when
     * refreshing analysis on the same database.
     */
    public void clearCache() {
        log.info("Clearing column result cache with {} entries", columnResultCache.size());
        columnResultCache.clear();
    }
}