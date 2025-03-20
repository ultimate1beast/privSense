package com.cgi.privsense.dbscanner.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents a sample of data from a table.
 */
@Getter
@Builder
@ToString
public class DataSample implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Name of the table.
     */
    private final String tableName;

    /**
     * List of column names.
     */
    private final List<String> columnNames;

    /**
     * List of rows, where each row is a map of column name to value.
     */
    private final List<Map<String, Object>> rows;

    /**
     * Total number of rows in the table.
     */
    private final int totalRows;

    /**
     * Number of rows in this sample.
     */
    private final int sampleSize;

    /**
     * Creates a data sample from a list of rows.
     *
     * @param tableName Table name
     * @param rows List of rows
     * @return Data sample
     */
    public static DataSample fromRows(String tableName, List<Map<String, Object>> rows) {
        return DataSample.builder()
                .tableName(tableName)
                .rows(rows)
                .columnNames(rows.isEmpty() ? List.of() : new ArrayList<>(rows.getFirst().keySet()))
                .totalRows(rows.size())
                .sampleSize(rows.size())
                .build();
    }

    /**
     * Constructor with table name and rows.
     *
     * @param tableName Table name
     * @param rows List of rows
     */
    public DataSample(String tableName, List<Map<String, Object>> rows) {
        this.tableName = tableName;
        this.rows = rows;
        this.columnNames = rows.isEmpty() ? List.of() : new ArrayList<>(rows.getFirst().keySet());
        this.totalRows = rows.size();
        this.sampleSize = rows.size();
    }

    /**
     * Constructor with all properties.
     *
     * @param tableName Table name
     * @param columnNames List of column names
     * @param rows List of rows
     * @param totalRows Total number of rows in the table
     * @param sampleSize Number of rows in this sample
     */
    public DataSample(String tableName, List<String> columnNames, List<Map<String, Object>> rows, int totalRows, int sampleSize) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.rows = rows;
        this.totalRows = totalRows;
        this.sampleSize = sampleSize;
    }
}