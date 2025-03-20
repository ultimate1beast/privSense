package com.cgi.privsense.dbscanner.config;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * A data source that routes to different target data sources based on a lookup key.
 * The lookup key is stored in a ThreadLocal variable.
 */
public class RoutingDataSource extends AbstractRoutingDataSource {

    /**
     * ThreadLocal to store the current data source name.
     */
    private static final ThreadLocal<String> currentDataSource = new ThreadLocal<>();

    /**
     * Sets the current data source name.
     *
     * @param dataSourceName Data source name
     */
    public static void setCurrentDataSource(String dataSourceName) {
        currentDataSource.set(dataSourceName);
    }

    /**
     * Clears the current data source name.
     */
    public static void clearCurrentDataSource() {
        currentDataSource.remove();
    }

    /**
     * Determines the current lookup key.
     * This is the data source name stored in the ThreadLocal.
     *
     * @return The current lookup key
     */
    @Override
    protected Object determineCurrentLookupKey() {
        return currentDataSource.get();
    }
}