package com.cgi.privsense.piidetector.service;

import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.PIITypeDetection;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service responsible for cleaning detection results by eliminating
 * redundant detections and selecting the most likely PII type.
 */
@Component
public class DetectionResultCleaner {
    private static final Logger log = LoggerFactory.getLogger(DetectionResultCleaner.class);

    /**
     * Cleans detections for a column by eliminating redundancies
     * and selecting the most reliable detections.
     *
     * @param columnInfo Column information to clean
     * @return Column information with cleaned detections
     */
    public ColumnPIIInfo cleanDetections(ColumnPIIInfo columnInfo) {
        if (columnInfo == null || !columnInfo.isPiiDetected() || columnInfo.getDetections().isEmpty()) {
            return columnInfo;
        }

        List<PIITypeDetection> originalDetections = new ArrayList<>(columnInfo.getDetections());
        List<PIITypeDetection> cleanedDetections = new ArrayList<>();

        // Group detections by PII type
        Map<PIIType, List<PIITypeDetection>> detectionsByType = originalDetections.stream()
                .collect(Collectors.groupingBy(PIITypeDetection::getPiiType));

        // For each PII type, select the detection with the highest confidence
        for (Map.Entry<PIIType, List<PIITypeDetection>> entry : detectionsByType.entrySet()) {
            PIIType piiType = entry.getKey();
            List<PIITypeDetection> detections = entry.getValue();

            // Find the detection with the highest confidence
            PIITypeDetection bestDetection = detections.stream()
                    .max(Comparator.comparing(PIITypeDetection::getConfidence))
                    .orElse(null);

            if (bestDetection != null) {
                cleanedDetections.add(bestDetection);
            }
        }

        log.debug("Cleaning detections for {}.{}: {} detections reduced to {}",
                columnInfo.getTableName(), columnInfo.getColumnName(),
                originalDetections.size(), cleanedDetections.size());

        // Create a new ColumnPIIInfo with cleaned detections
        // Make sure additionalInfo is never null
        Map<String, Object> additionalInfo = columnInfo.getAdditionalInfo();
        if (additionalInfo == null) {
            additionalInfo = new HashMap<>();
        }

        ColumnPIIInfo cleanedColumnInfo = ColumnPIIInfo.builder()
                .columnName(columnInfo.getColumnName())
                .tableName(columnInfo.getTableName())
                .columnType(columnInfo.getColumnType())
                .piiDetected(!cleanedDetections.isEmpty())
                .detections(cleanedDetections)
                .additionalInfo(additionalInfo)
                .build();

        return cleanedColumnInfo;
    }

    /**
     * Determines the most likely PII type for a column.
     *
     * @param columnInfo Column information
     * @return The most likely PII type or UNKNOWN if not detected
     */
    public PIIType determineMostLikelyPIIType(ColumnPIIInfo columnInfo) {
        if (columnInfo == null || !columnInfo.isPiiDetected() || columnInfo.getDetections().isEmpty()) {
            return PIIType.UNKNOWN;
        }

        // Find the detection with the highest confidence
        return columnInfo.getDetections().stream()
                .max(Comparator.comparing(PIITypeDetection::getConfidence))
                .map(PIITypeDetection::getPiiType)
                .orElse(PIIType.UNKNOWN);
    }
}