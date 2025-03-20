package com.cgi.privsense.piidetector.model;

import com.cgi.privsense.piidetector.model.enums.PIIType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnPIIInfo {
    private String columnName;
    private String tableName;
    private String columnType;
    private boolean piiDetected;

    @Builder.Default
    private List<PIITypeDetection> detections = new ArrayList<>();

    @Builder.Default
    private Map<String, Object> additionalInfo = new HashMap<>();

    /**
     * Adds a detection and updates the state.
     *
     * @param detection Detection to add
     */
    public void addDetection(PIITypeDetection detection) {
        if (detections == null) {
            detections = new ArrayList<>();
        }
        detections.add(detection);
        piiDetected = true;
    }

    /**
     * Gets the PII type with the highest confidence.
     *
     * @return The most likely PII type
     */
    public PIIType getMostLikelyPIIType() {
        if (detections == null || detections.isEmpty()) {
            return PIIType.UNKNOWN;
        }

        return detections.stream()
                .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()))
                .map(PIITypeDetection::getPiiType)
                .orElse(PIIType.UNKNOWN);
    }

    /**
     * Ensures the additionalInfo map is never null.
     */
    public Map<String, Object> getAdditionalInfo() {
        if (additionalInfo == null) {
            additionalInfo = new HashMap<>();
        }
        return additionalInfo;
    }
}
