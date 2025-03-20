/*
 * PIIDetector.java - Main interface for PII detection
 */
package com.cgi.privsense.piidetector.api;

import com.cgi.privsense.piidetector.model.PIIDetectionResult;
import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.TablePIIInfo;

import java.util.Map;

/**
 * Main interface for Personally Identifiable Information (PII) detection
 * in databases.
 */
public interface PIIDetector {
    /**
     * Detects PII across an entire database.
     *
     * @param connectionId Database connection identifier
     * @param dbType Database type (mysql, postgresql, etc.)
     * @return Detection result containing information about PIIs found
     */
    PIIDetectionResult detectPII(String connectionId, String dbType);

    /**
     * Detects PII in a specific table.
     *
     * @param connectionId Database connection identifier
     * @param dbType Database type
     * @param tableName Name of the table to analyze
     * @return Information about PIIs found in the table
     */
    TablePIIInfo detectPIIInTable(String connectionId, String dbType, String tableName);

    /**
     * Detects PII in a specific column.
     *
     * @param connectionId Database connection identifier
     * @param dbType Database type
     * @param tableName Table name
     * @param columnName Name of the column to analyze
     * @return Information about PIIs found in the column
     */
    ColumnPIIInfo detectPIIInColumn(String connectionId, String dbType, String tableName, String columnName);

    /**
     * Sets the minimum confidence level to consider data as PII.
     *
     * @param confidenceThreshold Confidence threshold (between 0.0 and 1.0)
     */
    void setConfidenceThreshold(double confidenceThreshold);

    /**
     * Enables or disables specific detection strategies.
     *
     * @param strategyMap Map of strategies and their activation state
     */
    void configureStrategies(Map<String, Boolean> strategyMap);
}