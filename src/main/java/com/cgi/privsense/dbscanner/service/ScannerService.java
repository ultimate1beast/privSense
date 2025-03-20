package com.cgi.privsense.dbscanner.service;

import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.model.RelationshipMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;

import java.util.List;
import java.util.Map;

/**
 * Interface for the scanner service.
 * Provides methods to scan and sample database structures and data.
 */
public interface ScannerService {
    /**
     * Scans all tables in a database.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @return List of table metadata
     */
    List<TableMetadata> scanTables(String dbType, String dataSourceName);

    /**
     * Scans a specific table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @return Table metadata
     */
    TableMetadata scanTable(String dbType, String dataSourceName, String tableName);

    /**
     * Scans the columns of a table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @return List of column metadata
     */
    List<ColumnMetadata> scanColumns(String dbType, String dataSourceName, String tableName);

    /**
     * Samples data from a table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @param limit Maximum number of rows
     * @return Data sample
     */
    DataSample sampleData(String dbType, String dataSourceName, String tableName, int limit);

    /**
     * Samples data from a column.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @param columnName Column name
     * @param limit Maximum number of values
     * @return List of sampled values
     */
    List<Object> sampleColumnData(String dbType, String dataSourceName, String tableName, String columnName, int limit);

    /**
     * Gets detailed relationships for a table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @return List of relationship metadata
     */
    List<RelationshipMetadata> getTableRelationships(String dbType, String dataSourceName, String tableName);


}