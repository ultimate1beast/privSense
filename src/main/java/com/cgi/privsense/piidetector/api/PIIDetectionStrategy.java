package com.cgi.privsense.piidetector.api;

import com.cgi.privsense.piidetector.model.ColumnPIIInfo;

import java.util.List;

/**
 * Interface for PII detection strategies.
 * Implements the Strategy pattern.
 */
public interface PIIDetectionStrategy {
    /**
     * Unique name of the strategy.
     *
     * @return Strategy name
     */
    String getName();

    /**
     * Detects PII in a specific column.
     *
     * @param connectionId Connection identifier
     * @param dbType Database type
     * @param tableName Table name
     * @param columnName Column name
     * @param sampleData Data sample (can be null if not available)
     * @return Detection result for this column
     */
    ColumnPIIInfo detectColumnPII(String connectionId, String dbType, String tableName,
                                  String columnName, List<Object> sampleData);

    /**
     * Indicates if this strategy can be applied with the available information.
     *
     * @param hasMetadata Indicates if metadata is available
     * @param hasSampleData Indicates if data samples are available
     * @return true if the strategy can be applied
     */
    boolean isApplicable(boolean hasMetadata, boolean hasSampleData);

    /**
     * Sets the confidence threshold for this strategy.
     *
     * @param threshold Threshold between 0.0 and 1.0
     */
    void setConfidenceThreshold(double threshold);
}