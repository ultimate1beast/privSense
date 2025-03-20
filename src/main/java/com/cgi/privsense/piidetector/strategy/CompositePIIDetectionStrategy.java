/*
 * CompositePIIDetectionStrategy.java - Optimized composite strategy for PII detection
 */
package com.cgi.privsense.piidetector.strategy;

import com.cgi.privsense.piidetector.api.PIIDetectionStrategy;
import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.enums.DetectionMethod;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import com.cgi.privsense.piidetector.model.PIITypeDetection;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Composite strategy that combines results from multiple strategies.
 * Implements the Composite pattern.
 */
@Component
public class CompositePIIDetectionStrategy extends AbstractPIIDetectionStrategy {
    private final List<PIIDetectionStrategy> strategies = new ArrayList<>();

    // Cache for detection results
    private final Map<String, ColumnPIIInfo> resultCache = new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return "CompositeStrategy";
    }

    /**
     * Adds a strategy to the composition.
     *
     * @param strategy Strategy to add
     */
    public void addStrategy(PIIDetectionStrategy strategy) {
        strategies.add(strategy);
    }

    @Override
    public ColumnPIIInfo detectColumnPII(String connectionId, String dbType, String tableName,
                                         String columnName, List<Object> sampleData) {
        // Generate cache key
        String cacheKey = strategies.stream()
                .map(PIIDetectionStrategy::getName)
                .sorted()
                .reduce("", (a, b) -> a + ":" + b) + ":" +
                dbType + ":" + tableName + ":" + columnName + ":" +
                (sampleData != null ? sampleData.hashCode() : "null");

        // Check cache first
        if (resultCache.containsKey(cacheKey)) {
            return resultCache.get(cacheKey);
        }

        ColumnPIIInfo result = ColumnPIIInfo.builder()
                .columnName(columnName)
                .tableName(tableName)
                .piiDetected(false)
                .detections(new ArrayList<>())
                .build();

        // Apply each strategy
        for (PIIDetectionStrategy strategy : strategies) {
            boolean hasMetadata = true;
            boolean hasSampleData = sampleData != null && !sampleData.isEmpty();

            if (strategy.isApplicable(hasMetadata, hasSampleData)) {
                ColumnPIIInfo strategyResult = strategy.detectColumnPII(
                        connectionId, dbType, tableName, columnName, sampleData);

                // Merge results with a voting approach
                if (strategyResult.isPiiDetected()) {
                    // Aggregate detections by PII type
                    Map<PIIType, List<PIITypeDetection>> detectionsByType = new HashMap<>();

                    for (PIITypeDetection detection : strategyResult.getDetections()) {
                        detectionsByType.computeIfAbsent(detection.getPiiType(), k -> new ArrayList<>())
                                .add(detection);
                    }

                    // Calculate average confidence for each PII type
                    for (Map.Entry<PIIType, List<PIITypeDetection>> entry : detectionsByType.entrySet()) {
                        PIIType piiType = entry.getKey();
                        List<PIITypeDetection> detections = entry.getValue();

                        double avgConfidence = detections.stream()
                                .mapToDouble(PIITypeDetection::getConfidence)
                                .average()
                                .orElse(0.0);

                        // If average confidence is sufficient, add a detection
                        if (avgConfidence >= confidenceThreshold) {
                            result.addDetection(createDetection(
                                    piiType,
                                    avgConfidence,
                                    DetectionMethod.COMPOSITE.name()));
                        }
                    }
                }
            }
        }

        // Store in cache
        resultCache.put(cacheKey, result);

        return result;
    }

    @Override
    public boolean isApplicable(boolean hasMetadata, boolean hasSampleData) {
        // The composite strategy is applicable if at least one of its strategies is
        return strategies.stream()
                .anyMatch(strategy -> strategy.isApplicable(hasMetadata, hasSampleData));
    }

    @Override
    public void setConfidenceThreshold(double threshold) {
        super.setConfidenceThreshold(threshold);
        // Propagate threshold to all strategies
        strategies.forEach(strategy -> strategy.setConfidenceThreshold(threshold));
        // Clear cache when threshold changes
        resultCache.clear();
    }

    /**
     * Clears the result cache.
     */
    public void clearCache() {
        resultCache.clear();
    }
}