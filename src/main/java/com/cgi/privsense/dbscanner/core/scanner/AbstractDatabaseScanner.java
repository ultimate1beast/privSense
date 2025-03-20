package com.cgi.privsense.dbscanner.core.scanner;

import com.cgi.privsense.dbscanner.exception.DatabaseScannerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.function.Function;

/**
 * Abstract base class for database scanners.
 * Provides common functionality for all database scanners.
 */
public abstract class AbstractDatabaseScanner implements DatabaseScanner {
    /**
     * The JDBC template used to execute SQL queries.
     */
    protected final JdbcTemplate jdbcTemplate;

    /**
     * The database type.
     */
    protected final String dbType;

    /**
     * Logger for this class.
     */
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Constructor.
     *
     * @param dataSource The data source
     * @param dbType The database type
     */
    protected AbstractDatabaseScanner(DataSource dataSource, String dbType) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.dbType = dbType;
        configureJdbcTemplate();
    }

    /**
     * Configures the JDBC template.
     */
    private void configureJdbcTemplate() {
        // Common JDBC template configuration
        jdbcTemplate.setFetchSize(1000);
        jdbcTemplate.setMaxRows(10000);
        jdbcTemplate.setQueryTimeout(30);
    }

    /**
     * Gets the database type.
     *
     * @return The database type
     */
    @Override
    public String getDatabaseType() {
        return dbType;
    }

    /**
     * Executes a query with standardized error handling.
     *
     * @param operationName Operation name for logging
     * @param query Function that executes the query
     * @return Query result
     * @throws DatabaseScannerException On error
     */
    protected <T> T executeQuery(String operationName, Function<JdbcTemplate, T> query) {
        try {
            logger.debug("Executing operation: {}", operationName);
            T result = query.apply(jdbcTemplate);
            logger.debug("Operation completed successfully: {}", operationName);
            return result;
        } catch (DataAccessException e) {
            logger.error("Database error executing {}: {}", operationName, e.getMessage(), e);
            throw new DatabaseScannerException("Error during " + operationName, e);
        } catch (Exception e) {
            logger.error("Unexpected error executing {}: {}", operationName, e.getMessage(), e);
            throw new DatabaseScannerException("Unexpected error during " + operationName, e);
        }
    }

    /**
     * Checks if a table exists.
     *
     * @param tableName Table name
     * @return true if the table exists, false otherwise
     */
    protected boolean tableExists(String tableName) {
        validateTableName(tableName);
        try {
            String sql = String.format("SELECT 1 FROM %s WHERE 1 = 0", escapeIdentifier(tableName));
            jdbcTemplate.queryForObject(sql, Integer.class);
            return true;
        } catch (Exception e) {
            logger.debug("Table doesn't exist or is not accessible: {}", tableName);
            return false;
        }
    }

    /**
     * Escapes an SQL identifier to prevent SQL injection.
     * This method should be overridden by specific implementations.
     *
     * @param identifier Identifier to escape
     * @return Escaped identifier
     */
    protected abstract String escapeIdentifier(String identifier);
}