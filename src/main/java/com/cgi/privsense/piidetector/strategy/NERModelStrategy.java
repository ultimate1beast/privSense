/*
 * NERModelStrategy.java - Optimized strategy using NER for PII detection
 */
package com.cgi.privsense.piidetector.strategy;

import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.enums.DetectionMethod;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import com.cgi.privsense.piidetector.service.NERServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Strategy using an external NER (Named Entity Recognition) service
 * to detect named entities in data.
 */
@Component
public class NERModelStrategy extends AbstractPIIDetectionStrategy {
    private static final Logger log = LoggerFactory.getLogger(NERModelStrategy.class);

    private final NERServiceClient nerServiceClient;

    // Cache for service availability status
    private Boolean serviceAvailable = null;
    private long lastAvailabilityCheck = 0;
    private static final long AVAILABILITY_CHECK_INTERVAL = 60000; // 1 minute

    // Cache for detection results
    private final Map<String, ColumnPIIInfo> resultCache = new ConcurrentHashMap<>();

    // Minimum number of samples to use NER (to avoid overhead for small datasets)
    private static final int MIN_SAMPLES_FOR_NER = 3;

    @Autowired
    public NERModelStrategy(NERServiceClient nerServiceClient) {
        this.nerServiceClient = nerServiceClient;
    }

    @Override
    public String getName() {
        return "NERModelStrategy";
    }

    @Override
    public ColumnPIIInfo detectColumnPII(String connectionId, String dbType, String tableName,
                                         String columnName, List<Object> sampleData) {
        // Check service availability before proceeding
        if (!isServiceAvailable()) {
            log.warn("NER service not available, skipping NER analysis for {}.{}", tableName, columnName);
            return ColumnPIIInfo.builder()
                    .columnName(columnName)
                    .tableName(tableName)
                    .piiDetected(false)
                    .detections(new ArrayList<>())
                    .build();
        }

        // Generate cache key
        String cacheKey = dbType + ":" + tableName + ":" + columnName + ":" +
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

        if (sampleData == null || sampleData.isEmpty() || sampleData.size() < MIN_SAMPLES_FOR_NER) {
            return result;
        }

        try {
            // Prepare data for NER analysis
            List<String> textSamples = sampleData.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .filter(s -> !s.trim().isEmpty())
                    .map(value -> String.format("The value of field '%s' is: %s", columnName, value))
                    .collect(Collectors.toList());

            if (textSamples.isEmpty()) {
                return result;
            }

            // Measure response time
            long startTime = System.currentTimeMillis();

            // Call NER service
            Map<String, Double> nerResults = nerServiceClient.analyzeText(textSamples);

            long responseTime = System.currentTimeMillis() - startTime;

            // Convert NER results to PII detections
            for (Map.Entry<String, Double> entry : nerResults.entrySet()) {
                String entityType = entry.getKey();
                Double confidence = entry.getValue();

                if (confidence >= confidenceThreshold) {
                    PIIType piiType = mapNerEntityToPiiType(entityType);

                    // Create detection
                    var detection = createDetection(
                            piiType,
                            confidence,
                            DetectionMethod.NER_MODEL.name());

                    // Add detailed metadata
                    detection.getDetectionMetadata().put("sampleSize", textSamples.size());
                    detection.getDetectionMetadata().put("responseTimeMs", responseTime);
                    detection.getDetectionMetadata().put("entityType", entityType);
                    detection.getDetectionMetadata().put("matchRatio", confidence);

                    // Add some sample examples (limit to 3)
                    List<String> sampleExamples = textSamples.stream()
                            .limit(3)
                            .collect(Collectors.toList());
                    detection.getDetectionMetadata().put("sampleExamples", sampleExamples);

                    // Add the detection to the result
                    result.addDetection(detection);
                }
            }

            // Store in cache
            resultCache.put(cacheKey, result);

        } catch (Exception e) {
            log.error("Error during NER analysis: {}", e.getMessage(), e);
            // Mark service as unavailable on error
            serviceAvailable = false;
        }

        return result;
    }

    @Override
    public boolean isApplicable(boolean hasMetadata, boolean hasSampleData) {
        // This strategy requires data samples and service availability
        return hasSampleData && isServiceAvailable();
    }

    /**
     * Maps a NER entity type to the corresponding PII type.
     *
     * @param nerEntityType NER entity type
     * @return Corresponding PII type
     */
    private PIIType mapNerEntityToPiiType(String nerEntityType) {
        // Extract the main type (after the I-, B-, etc. prefix)
        String type = nerEntityType;
        if (nerEntityType.contains("-")) {
            String[] parts = nerEntityType.split("-", 2);
            if (parts.length > 1) {
                type = parts[1];
            }
        }

        // Convert to uppercase for comparison
        String upperCaseType = type.toUpperCase();

        // Log for debugging
        log.debug("Mapping NER entity type: {} (extracted: {})", nerEntityType, upperCaseType);

        // Map to PII types based on specific entities from the model
        return switch (upperCaseType) {
            // Financial and banking identifiers
            case "ACCOUNTNUM" -> PIIType.BANK_ACCOUNT;
            case "CREDITCARDNUMBER" -> PIIType.CREDIT_CARD;
            case "TAXNUM" -> PIIType.NATIONAL_ID;  // Tax ID number

            // Addresses and location
            case "BUILDINGNUM" -> PIIType.ADDRESS;
            case "STREET" -> PIIType.ADDRESS;
            case "CITY" -> PIIType.CITY;
            case "ZIPCODE" -> PIIType.POSTAL_CODE;

            // Personal identifiers
            case "IDCARDNUM" -> PIIType.NATIONAL_ID;
            case "DRIVERLICENSENUM" -> PIIType.DRIVING_LICENSE;
            case "SOCIALNUM" -> PIIType.NATIONAL_ID;  // Social security number

            // Names
            case "GIVENNAME" -> PIIType.FIRST_NAME;
            case "SURNAME" -> PIIType.LAST_NAME;

            // Contact information
            case "EMAIL" -> PIIType.EMAIL;
            case "TELEPHONENUM" -> PIIType.PHONE_NUMBER;

            // Online information
            case "USERNAME" -> PIIType.USERNAME;
            case "PASSWORD" -> PIIType.PASSWORD;

            // Temporal data
            case "DATEOFBIRTH" -> PIIType.DATE_OF_BIRTH;

            case "SALARY" ->PIIType.SALARY;

            // If no other case matches
            default -> {
                log.warn("Unmapped NER entity type: {} -> UNKNOWN", nerEntityType);
                yield PIIType.UNKNOWN;
            }
        };
    }

    /**
     * Checks if the NER service is available.
     * Uses cached status to avoid frequent API calls.
     *
     * @return true if the service is available
     */
    public boolean isServiceAvailable() {
        long currentTime = System.currentTimeMillis();

        // Use cached result if recent
        if (serviceAvailable != null &&
                (currentTime - lastAvailabilityCheck) < AVAILABILITY_CHECK_INTERVAL) {
            return serviceAvailable;
        }

        // Check service availability and update cache
        serviceAvailable = nerServiceClient.isServiceAvailable();
        lastAvailabilityCheck = currentTime;

        return serviceAvailable;
    }

    /**
     * Clears the result cache.
     */
    public void clearCache() {
        resultCache.clear();
        // Reset service availability status
        serviceAvailable = null;
    }
}