package com.cgi.privsense.piidetector.model;

import com.cgi.privsense.piidetector.model.enums.PIIType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PIITypeDetection {
    private PIIType piiType;
    private double confidence;
    private String detectionMethod;

    @Builder.Default
    private Map<String, Object> detectionMetadata = new HashMap<>();

    /**
     * Ensures detectionMetadata is never null.
     */
    public Map<String, Object> getDetectionMetadata() {
        if (detectionMetadata == null) {
            detectionMetadata = new HashMap<>();
        }
        return detectionMetadata;
    }
}