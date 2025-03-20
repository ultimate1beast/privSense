package com.cgi.privsense.dbscanner.core.driver;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A JDBC Driver implementation that wraps another Driver.
 * This is used to avoid classloader issues when loading JDBC drivers dynamically.
 */
public class DriverShim implements Driver {
    /**
     * The wrapped driver.
     */
    private final Driver driver;

    /**
     * Creates a new DriverShim.
     *
     * @param driver The driver to wrap
     */
    public DriverShim(Driver driver) {
        this.driver = driver;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return driver.connect(url, info);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return driver.acceptsURL(url);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driver.getPropertyInfo(url, info);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean jdbcCompliant() {
        return driver.jdbcCompliant();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }
}