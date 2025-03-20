package com.cgi.privsense.dbscanner.service;

import com.cgi.privsense.dbscanner.core.datasource.DataSourceProvider;
import com.cgi.privsense.dbscanner.core.scanner.DatabaseScanner;
import com.cgi.privsense.dbscanner.core.scanner.DatabaseScannerFactory;
import com.cgi.privsense.dbscanner.exception.ScannerException;
import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.model.RelationshipMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;


/**
 * Service for scanning databases.
 */
@Service
public class DatabaseScannerService implements ScannerService {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseScannerService.class);

    /**
     * Factory for creating database scanners.
     */
    private final DatabaseScannerFactory scannerFactory;

    /**
     * Provider for data sources.
     */
    private final DataSourceProvider dataSourceProvider;

    /**
     * Constructor.
     *
     * @param scannerFactory Factory for creating database scanners
     * @param dataSourceProvider Provider for data sources
     */
    public DatabaseScannerService(
            DatabaseScannerFactory scannerFactory,
            DataSourceProvider dataSourceProvider) {
        this.scannerFactory = scannerFactory;
        this.dataSourceProvider = dataSourceProvider;
    }

    /**
     * Gets a scanner for the specified database type and data source.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @return Database scanner
     */
    private DatabaseScanner getScanner(String dbType, String dataSourceName) {
        logger.debug("Getting scanner for db type: {} and datasource: {}", dbType, dataSourceName);
        DataSource dataSource = dataSourceProvider.getDataSource(dataSourceName);
        if (dataSource == null) {
            throw new ScannerException("DataSource not found: " + dataSourceName);
        }
        return scannerFactory.getScanner(dbType, dataSource);
    }

    /**
     * Scans all tables in a database.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @return List of table metadata
     */
    @Override
    @Cacheable(value = "tableMetadata", key = "#dbType + '-' + #dataSourceName")
    public List<TableMetadata> scanTables(String dbType, String dataSourceName) {
        try {
            logger.info("Scanning tables for: {}/{}", dbType, dataSourceName);
            return getScanner(dbType, dataSourceName).scanTables();
        } catch (Exception e) {
            logger.error("Error scanning tables: {}", e.getMessage(), e);
            throw new ScannerException("Failed to scan tables", e);
        }
    }

    /**
     * Scans a specific table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @return Table metadata
     */
    @Override
    @Cacheable(value = "tableMetadata", key = "#dbType + '-' + #dataSourceName + '-' + #tableName")
    public TableMetadata scanTable(String dbType, String dataSourceName, String tableName) {
        try {
            logger.info("Scanning table: {}/{}/{}", dbType, dataSourceName, tableName);
            return getScanner(dbType, dataSourceName).scanTable(tableName);
        } catch (Exception e) {
            logger.error("Error scanning table {}: {}", tableName, e.getMessage(), e);
            throw new ScannerException("Failed to scan table: " + tableName, e);
        }
    }

    /**
     * Scans the columns of a table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @return List of column metadata
     */
    @Override
    @Cacheable(value = "columnMetadata", key = "#dbType + '-' + #dataSourceName + '-' + #tableName")
    public List<ColumnMetadata> scanColumns(String dbType, String dataSourceName, String tableName) {
        try {
            logger.info("Scanning columns for table: {}/{}/{}", dbType, dataSourceName, tableName);
            return getScanner(dbType, dataSourceName).scanColumns(tableName);
        } catch (Exception e) {
            logger.error("Error scanning columns for table {}: {}", tableName, e.getMessage(), e);
            throw new ScannerException("Failed to scan columns for table: " + tableName, e);
        }
    }

    /**
     * Samples data from a table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @param limit Maximum number of rows
     * @return Data sample
     */
    @Override
    public DataSample sampleData(String dbType, String dataSourceName, String tableName, int limit) {
        try {
            logger.info("Sampling data from table: {}/{}/{} (limit: {})", dbType, dataSourceName, tableName, limit);
            return getScanner(dbType, dataSourceName).sampleTableData(tableName, limit);
        } catch (Exception e) {
            logger.error("Error sampling data from table {}: {}", tableName, e.getMessage(), e);
            throw new ScannerException("Failed to sample data from table: " + tableName, e);
        }
    }

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
    @Override
    public List<Object> sampleColumnData(String dbType, String dataSourceName, String tableName, String columnName, int limit) {
        try {
            logger.info("Sampling data from column: {}/{}/{}/{} (limit: {})",
                    dbType, dataSourceName, tableName, columnName, limit);
            return getScanner(dbType, dataSourceName).sampleColumnData(tableName, columnName, limit);
        } catch (Exception e) {
            logger.error("Error sampling data from column {}.{}: {}", tableName, columnName, e.getMessage(), e);
            throw new ScannerException("Failed to sample data from column: " + tableName + "." + columnName, e);
        }
    }

    /**
     * Gets detailed relationships for a table.
     *
     * @param dbType Database type
     * @param dataSourceName Data source name
     * @param tableName Table name
     * @return List of relationship metadata
     */
    @Override
   // @Cacheable(value = "relationshipMetadata", key = "#dbType + '-' + #dataSourceName + '-' + #tableName")
    public List<RelationshipMetadata> getTableRelationships(String dbType, String dataSourceName, String tableName) {
        try {
            logger.info("Getting relationships for table: {}/{}/{}", dbType, dataSourceName, tableName);
            return getScanner(dbType, dataSourceName).getTableRelationships(tableName);
        } catch (Exception e) {
            logger.error("Error getting relationships for table {}: {}", tableName, e.getMessage(), e);
            throw new ScannerException("Failed to get relationships for table: " + tableName, e);
        }
    }



}