/*
 * AbstractPIIDetectionStrategy.java - Abstract strategy for PII detection
 */
package com.cgi.privsense.piidetector.strategy;

import com.cgi.privsense.piidetector.api.PIIDetectionStrategy;
import com.cgi.privsense.piidetector.model.*;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Base strategy for PII detection.
 * Provides common functionality for all strategies.
 */
public abstract class AbstractPIIDetectionStrategy implements PIIDetectionStrategy {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected double confidenceThreshold = 0.7;

    @Override
    public void setConfidenceThreshold(double threshold) {
        this.confidenceThreshold = threshold;
    }

    /**
     * Creates a PII detection with initialized detection metadata.
     *
     * @param type PII type
     * @param confidence Confidence level
     * @param method Detection method
     * @return PII detection object
     */
    protected PIITypeDetection createDetection(PIIType type, double confidence, String method) {
        // Ensure detectionMetadata is initialized with an empty HashMap
        Map<String, Object> metadata = new HashMap<>();

        // Add basic metadata common to all strategies
        metadata.put("confidenceThreshold", confidenceThreshold);
        metadata.put("timestamp", System.currentTimeMillis());

        return PIITypeDetection.builder()
                .piiType(type)
                .confidence(confidence)
                .detectionMethod(method)
                .detectionMetadata(metadata)  // Use prepared map with initial values
                .build();
    }

    /**
     * Creates a PII detection with custom metadata.
     *
     * @param type PII type
     * @param confidence Confidence level
     * @param method Detection method
     * @param metadata Additional metadata to include
     * @return PII detection object
     */
    protected PIITypeDetection createDetection(PIIType type, double confidence, String method, Map<String, Object> metadata) {
        // Ensure metadata is not null
        Map<String, Object> safeMetadata = metadata != null ? metadata : new HashMap<>();

        // Add basic metadata common to all strategies
        safeMetadata.put("confidenceThreshold", confidenceThreshold);
        safeMetadata.put("timestamp", System.currentTimeMillis());

        return PIITypeDetection.builder()
                .piiType(type)
                .confidence(confidence)
                .detectionMethod(method)
                .detectionMetadata(safeMetadata)
                .build();
    }
}