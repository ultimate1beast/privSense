package com.cgi.privsense.dbscanner.core.driver;

import com.cgi.privsense.dbscanner.exception.DriverException;

import java.nio.file.Path;
import java.sql.Driver;

/**
 * Interface for managing database drivers.
 * Responsible for dynamically loading JDBC drivers.
 */
public interface DriverManager {
    /**
     * Ensures that the specified driver is available.
     * Downloads and loads the driver if necessary.
     *
     * @param driverClassName The fully qualified class name of the driver
     * @throws DriverException If the driver cannot be loaded
     */
    void ensureDriverAvailable(String driverClassName) throws DriverException;

    /**
     * Checks if a driver is already loaded.
     *
     * @param driverClassName The fully qualified class name of the driver
     * @return true if the driver is already loaded, false otherwise
     */
    boolean isDriverLoaded(String driverClassName);

    /**
     * Gets a loaded driver.
     *
     * @param driverClassName The fully qualified class name of the driver
     * @return The driver instance or null if not loaded
     */
    Driver getDriver(String driverClassName);
}