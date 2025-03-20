package com.cgi.privsense.dbscanner.core.datasource;

import com.cgi.privsense.dbscanner.config.dtoconfig.DatabaseConnectionRequest;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * Interface for managing data sources.
 */
public interface DataSourceProvider {
    /**
     * Gets an existing data source.
     *
     * @param name Name of the data source
     * @return The data source, or null if not found
     */
    DataSource getDataSource(String name);

    /**
     * Creates a new data source.
     *
     * @param request Connection configuration
     * @return The created data source
     */
    DataSource createDataSource(DatabaseConnectionRequest request);

    /**
     * Registers a data source.
     *
     * @param name Name of the data source
     * @param dataSource The data source
     */
    void registerDataSource(String name, DataSource dataSource);

    /**
     * Gets the database type for a connection.
     *
     * @param connectionId Connection ID
     * @return The database type (mysql, postgresql, etc.)
     */
    String getDatabaseType(String connectionId);


    /**
     * Checks if a data source with the given name exists.
     *
     * @param name Data source name
     * @return true if the data source exists, false otherwise
     */
    boolean hasDataSource(String name);

    /**
     * Removes a registered data source.
     *
     * @param name Name of the data source to remove
     * @return true if the data source was removed, false otherwise
     */
    boolean removeDataSource(String name);

    /**
     * Gets information about all registered data sources.
     *
     * @return List of data source information
     */
    List<Map<String, Object>> getDataSourcesInfo();
}