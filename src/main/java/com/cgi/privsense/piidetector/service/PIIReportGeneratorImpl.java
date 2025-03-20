/*
 * PIIReportGeneratorImpl.java - Optimized implementation for PII report generation
 */
package com.cgi.privsense.piidetector.service;

import com.cgi.privsense.piidetector.api.PIIReportGenerator;
import com.cgi.privsense.piidetector.model.*;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of the PII report generator.
 */
@Component
public class PIIReportGeneratorImpl implements PIIReportGenerator {
    private static final Logger log = LoggerFactory.getLogger(PIIReportGeneratorImpl.class);

    // Object mapper for JSON serialization
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String generateReport(PIIDetectionResult result) {
        StringBuilder report = new StringBuilder();
        report.append("== PII Detection Report ==\n\n");
        report.append(String.format("Connection: %s (Type: %s)\n", result.getConnectionId(), result.getDbType()));
        report.append(String.format("Total analysis time: %.2f seconds\n", result.getProcessingTimeMs() / 1000.0));
        report.append(String.format("Total PIIs detected: %d\n\n", result.getTotalPiiCount()));

        // Summary by PII type
        report.append("Detected PII types:\n");
        result.getPiiTypeCounts().entrySet().stream()
                .sorted(Map.Entry.<PIIType, Integer>comparingByValue().reversed())
                .forEach(entry -> {
                    report.append(String.format("- %s: %d\n", entry.getKey(), entry.getValue()));
                });
        report.append("\n");

        // Details by table
        report.append("Details by table:\n");
        for (TablePIIInfo table : result.getTableResults()) {
            report.append(String.format("\nTable: %s (PII: %s, Density: %.2f%%)\n",
                    table.getTableName(),
                    table.isHasPii() ? "Yes" : "No",
                    table.getPiiDensity() * 100));

            // Group columns by PII type for better organization
            Map<PIIType, List<ColumnPIIInfo>> columnsByPiiType = table.getColumnResults().stream()
                    .filter(ColumnPIIInfo::isPiiDetected)
                    .collect(Collectors.groupingBy(ColumnPIIInfo::getMostLikelyPIIType));

            // Display columns grouped by PII type
            columnsByPiiType.forEach((piiType, columns) -> {
                report.append(String.format("  * %s PII (%d columns):\n", piiType, columns.size()));

                for (ColumnPIIInfo column : columns) {
                    report.append(String.format("    - Column: %s (%s)\n", column.getColumnName(), column.getColumnType()));

                    // Show top detection for this column
                    column.getDetections().stream()
                            .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()))
                            .ifPresent(detection -> {
                                report.append(String.format("      Confidence: %.2f%%, Method: %s\n",
                                        detection.getConfidence() * 100,
                                        detection.getDetectionMethod()));
                            });
                }
            });
        }

        return report.toString();
    }

    @Override
    public String generateSummary(PIIDetectionResult result) {
        StringBuilder summary = new StringBuilder();
        summary.append("== PII Detection Summary ==\n\n");

        // General statistics
        int totalTables = result.getTableResults().size();
        long tablesWithPii = result.getTableResults().stream()
                .filter(TablePIIInfo::isHasPii)
                .count();

        summary.append(String.format("Database: %s (%s)\n", result.getConnectionId(), result.getDbType()));
        summary.append(String.format("Tables analyzed: %d\n", totalTables));
        summary.append(String.format("Tables containing PII: %d (%.2f%%)\n",
                tablesWithPii, (double) tablesWithPii / totalTables * 100));
        summary.append(String.format("Total PIIs detected: %d\n\n", result.getTotalPiiCount()));

        // Top 5 most frequent PII types
        summary.append("Top PII types detected:\n");
        result.getPiiTypeCounts().entrySet().stream()
                .sorted(Map.Entry.<PIIType, Integer>comparingByValue().reversed())
                .limit(5)
                .forEach(entry -> {
                    summary.append(String.format("- %s: %d\n", entry.getKey(), entry.getValue()));
                });

        // Add performance metrics
        if (result.getAdditionalMetadata() != null &&
                result.getAdditionalMetadata().containsKey("performanceMetrics")) {

            @SuppressWarnings("unchecked")
            Map<String, Object> metrics = (Map<String, Object>) result.getAdditionalMetadata().get("performanceMetrics");

            summary.append("\nPerformance Metrics:\n");
            summary.append(String.format("- Total processing time: %.2f seconds\n", result.getProcessingTimeMs() / 1000.0));
            summary.append(String.format("- Average time per column: %.2f ms\n", metrics.getOrDefault("averageColumnTimeMs", 0.0)));

            summary.append("\nDetection pipeline efficiency:\n");
            summary.append(String.format("- Early stops at heuristic stage: %.2f%%\n", metrics.getOrDefault("heuristicStopPercentage", 0.0)));
            summary.append(String.format("- Early stops at regex stage: %.2f%%\n", metrics.getOrDefault("regexStopPercentage", 0.0)));
            summary.append(String.format("- Columns requiring NER: %.2f%%\n", metrics.getOrDefault("nerStopPercentage", 0.0)));
            summary.append(String.format("- Skipped technical columns: %.2f%%\n", metrics.getOrDefault("skippedColumnsPercentage", 0.0)));
        }

        return summary.toString();
    }

    @Override
    public byte[] exportResults(PIIDetectionResult result, String format) {
        try {
            if ("json".equalsIgnoreCase(format)) {
                return exportToJson(result);
            } else if ("csv".equalsIgnoreCase(format)) {
                return exportToCsv(result);
            } else {
                throw new IllegalArgumentException("Unsupported format: " + format);
            }
        } catch (Exception e) {
            log.error("Error exporting results: {}", e.getMessage(), e);
            throw new RuntimeException("Error exporting results", e);
        }
    }

    /**
     * Exports results in JSON format.
     *
     * @param result Result to export
     * @return JSON data
     */
    private byte[] exportToJson(PIIDetectionResult result) throws Exception {
        // Create a simplified version of the data for export
        Map<String, Object> exportData = new HashMap<>();
        exportData.put("connectionId", result.getConnectionId());
        exportData.put("dbType", result.getDbType());
        exportData.put("totalPiiCount", result.getTotalPiiCount());
        exportData.put("processingTimeMs", result.getProcessingTimeMs());
        exportData.put("piiTypeCounts", result.getPiiTypeCounts());

        // Create a simplified table structure with only PII columns
        List<Map<String, Object>> tables = new ArrayList<>();
        for (TablePIIInfo table : result.getTableResults()) {
            if (table.isHasPii()) {
                Map<String, Object> tableData = new HashMap<>();
                tableData.put("tableName", table.getTableName());
                tableData.put("piiDensity", table.getPiiDensity());

                List<Map<String, Object>> piiColumns = new ArrayList<>();
                for (ColumnPIIInfo column : table.getColumnResults()) {
                    if (column.isPiiDetected()) {
                        Map<String, Object> columnData = new HashMap<>();
                        columnData.put("columnName", column.getColumnName());
                        columnData.put("columnType", column.getColumnType());
                        columnData.put("mostLikelyPiiType", column.getMostLikelyPIIType().name());

                        // Include detection with highest confidence
                        column.getDetections().stream()
                                .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()))
                                .ifPresent(detection -> {
                                    columnData.put("confidence", detection.getConfidence());
                                    columnData.put("detectionMethod", detection.getDetectionMethod());
                                });

                        piiColumns.add(columnData);
                    }
                }

                tableData.put("piiColumns", piiColumns);
                tables.add(tableData);
            }
        }

        exportData.put("tables", tables);

        // Convert to JSON using Jackson
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(exportData);
    }

    /**
     * Exports results in CSV format using semicolons as delimiters.
     *
     * @param result Result to export
     * @return CSV data
     */
    private byte[] exportToCsv(PIIDetectionResult result) throws Exception {
        StringBuilder csv = new StringBuilder();

        // Headers with semicolon delimiter
        csv.append("Table;Column;Type;PII Type;Confidence;Detection Method\n");

        // Data
        for (TablePIIInfo table : result.getTableResults()) {
            for (ColumnPIIInfo column : table.getColumnResults()) {
                if (column.isPiiDetected()) {
                    // Get most likely detection for summary purposes
                    PIITypeDetection bestDetection = column.getDetections().stream()
                            .max((d1, d2) -> Double.compare(d1.getConfidence(), d2.getConfidence()))
                            .orElse(null);

                    if (bestDetection != null) {
                        csv.append(escapeCsvField(table.getTableName())).append(";")
                                .append(escapeCsvField(column.getColumnName())).append(";")
                                .append(escapeCsvField(column.getColumnType())).append(";")
                                .append(escapeCsvField(bestDetection.getPiiType().name())).append(";")
                                .append(String.format("%.2f", bestDetection.getConfidence())).append(";")
                                .append(escapeCsvField(bestDetection.getDetectionMethod())).append("\n");
                    }
                }
            }
        }

        return csv.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Escapes CSV field.
     * When using semicolons as delimiters, we need to escape semicolons, quotes, and newlines.
     *
     * @param field Field to escape
     * @return Escaped field
     */
    private String escapeCsvField(String field) {
        if (field == null) {
            return "";
        }

        if (field.contains(";") || field.contains("\"") || field.contains("\n")) {
            return "\"" + field.replace("\"", "\"\"") + "\"";
        }

        return field;
    }
}