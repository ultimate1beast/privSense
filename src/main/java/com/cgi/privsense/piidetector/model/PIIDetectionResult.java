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
public class PIIDetectionResult {
    private String connectionId;
    private String dbType;

    @Builder.Default
    private List<TablePIIInfo> tableResults = new ArrayList<>();

    @Builder.Default
    private Map<PIIType, Integer> piiTypeCounts = new HashMap<>();

    private int totalPiiCount;
    private long processingTimeMs;

    @Builder.Default
    private Map<String, Object> additionalMetadata = new HashMap<>();

    /**
     * Adds table results and updates global statistics.
     *
     * @param tableResult Detection result for a table
     */
    public void addTableResult(TablePIIInfo tableResult) {
        if (tableResults == null) {
            tableResults = new ArrayList<>();
        }
        tableResults.add(tableResult);
        updateStatistics(tableResult);
    }

    private void updateStatistics(TablePIIInfo tableResult) {
        if (tableResult.getColumnResults() == null) {
            return;
        }

        for (ColumnPIIInfo columnResult : tableResult.getColumnResults()) {
            if (columnResult.isPiiDetected()) {
                totalPiiCount++;

                if (columnResult.getDetections() == null) {
                    continue;
                }

                for (PIITypeDetection detection : columnResult.getDetections()) {
                    PIIType type = detection.getPiiType();
                    if (piiTypeCounts == null) {
                        piiTypeCounts = new HashMap<>();
                    }
                    piiTypeCounts.put(type, piiTypeCounts.getOrDefault(type, 0) + 1);
                }
            }
        }
    }

    /**
     * Ensures the additionalMetadata map is never null.
     */
    public Map<String, Object> getAdditionalMetadata() {
        if (additionalMetadata == null) {
            additionalMetadata = new HashMap<>();
        }
        return additionalMetadata;
    }
}
