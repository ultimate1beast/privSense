package com.cgi.privsense.dbscanner.exception;

/**
 * Exception for data source-related errors.
 */
public class DataSourceException extends BaseException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new DataSourceException with the specified message.
     *
     * @param message Exception message
     */
    public DataSourceException(String message) {
        super(message, "DATASOURCE_ERROR");
    }

    /**
     * Creates a new DataSourceException with the specified message and cause.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     */
    public DataSourceException(String message, Throwable cause) {
        super(message, cause, "DATASOURCE_ERROR");
    }
}