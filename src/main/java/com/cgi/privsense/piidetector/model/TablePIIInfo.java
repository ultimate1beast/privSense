package com.cgi.privsense.piidetector.model;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TablePIIInfo {
    private String tableName;
    private List<ColumnPIIInfo> columnResults = new ArrayList<>();
    private boolean hasPii;
    private double piiDensity; // Percentage of columns containing PII

    /**
     * Adds a column result and updates statistics.
     *
     * @param columnResult Detection result for a column
     */
    public void addColumnResult(ColumnPIIInfo columnResult) {
        columnResults.add(columnResult);
        updateStatistics();
    }

    private void updateStatistics() {
        long piiColumns = columnResults.stream()
                .filter(ColumnPIIInfo::isPiiDetected)
                .count();

        hasPii = piiColumns > 0;
        piiDensity = columnResults.isEmpty() ? 0 :
                (double) piiColumns / columnResults.size();
    }
}