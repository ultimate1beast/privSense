package com.cgi.privsense.dbscanner.config;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * An empty implementation of DataSource that throws an exception when a connection is requested.
 * Used as a default data source when no data source is specified.
 */
public class EmptyDataSource implements DataSource {
    /**
     * Gets a connection with the given username and password.
     *
     * @param username Username
     * @param password Password
     * @return Never returns
     * @throws SQLException Always thrown
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    /**
     * Gets the log writer.
     *
     * @return null
     */
    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    /**
     * Sets the log writer.
     *
     * @param out Log writer
     */
    @Override
    public void setLogWriter(PrintWriter out) {
    }

    /**
     * Sets the login timeout.
     *
     * @param seconds Timeout in seconds
     */
    @Override
    public void setLoginTimeout(int seconds) {
    }

    /**
     * Gets the login timeout.
     *
     * @return 0
     */
    @Override
    public int getLoginTimeout() {
        return 0;
    }

    /**
     * Gets the parent logger.
     *
     * @return Never returns
     * @throws SQLFeatureNotSupportedException Always thrown
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Unwraps the data source.
     *
     * @param iface Interface to unwrap to
     * @return Never returns
     * @throws SQLException Always thrown
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not supported");
    }

    /**
     * Checks if the data source is a wrapper for the given interface.
     *
     * @param iface Interface to check
     * @return Always false
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    /*Gets a connection.
     *
     * @return Never returns
     * @throws SQLException Always thrown
     */
    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLException("No default connection available - please configure a database first");
    }

}