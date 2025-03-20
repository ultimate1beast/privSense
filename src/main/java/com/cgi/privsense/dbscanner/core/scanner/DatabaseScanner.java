package com.cgi.privsense.dbscanner.core.scanner;

import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.model.RelationshipMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;

import java.util.List;
import java.util.Map;

/**
 * Interface for database scanners.
 * Provides methods to scan and sample database structures and data.
 */
public interface DatabaseScanner {
    /**
     * Scans all available tables.
     *
     * @return List of table metadata
     */
    List<TableMetadata> scanTables();

    /**
     * Scans a specific table.
     *
     * @param tableName Table name
     * @return Table metadata
     */
    TableMetadata scanTable(String tableName);

    /**
     * Scans the columns of a table.
     *
     * @param tableName Table name
     * @return List of column metadata
     */
    List<ColumnMetadata> scanColumns(String tableName);

    /**
     * Samples data from a table.
     *
     * @param tableName Table name
     * @param limit Maximum number of rows
     * @return Data sample
     */
    DataSample sampleTableData(String tableName, int limit);

    /**
     * Samples data from a column.
     *
     * @param tableName Table name
     * @param columnName Column name
     * @param limit Maximum number of values
     * @return List of sampled values
     */
    List<Object> sampleColumnData(String tableName, String columnName, int limit);

    /**
     * Gets the detailed relationships of a table.
     *
     * @param tableName Table name
     * @return List of relationship metadata
     */
    List<RelationshipMetadata> getTableRelationships(String tableName);




    /**
     * Gets the database type.
     *
     * @return Database type (mysql, postgresql, etc.)
     */
    String getDatabaseType();

    /**
     * Validates a table name.
     *
     * @param tableName Table name to validate
     * @throws IllegalArgumentException If the name is invalid
     */
    default void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
    }

    /**
     * Validates a column name.
     *
     * @param columnName Column name to validate
     * @throws IllegalArgumentException If the name is invalid
     */
    default void validateColumnName(String columnName) {
        if (columnName == null || columnName.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
    }
}