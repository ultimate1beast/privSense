package com.cgi.privsense.piidetector.api;

import com.cgi.privsense.piidetector.model.PIIDetectionResult;

/**
 * Interface for generating reports on detected PIIs.
 */
public interface PIIReportGenerator {
    /**
     * Generates a comprehensive report on detected PIIs.
     *
     * @param result Detection result
     * @return Formatted report
     */
    String generateReport(PIIDetectionResult result);

    /**
     * Generates a summary of detected PIIs.
     *
     * @param result Detection result
     * @return Formatted summary
     */
    String generateSummary(PIIDetectionResult result);

    /**
     * Exports results in a specific format.
     *
     * @param result Detection result
     * @param format Export format (JSON, CSV, etc.)
     * @return Exported data
     */
    byte[] exportResults(PIIDetectionResult result, String format);
}
