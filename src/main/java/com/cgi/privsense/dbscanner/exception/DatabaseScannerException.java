package com.cgi.privsense.dbscanner.exception;

/**
 * Exception for database scanner errors.
 */
public class DatabaseScannerException extends BaseException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new DatabaseScannerException with the specified message.
     *
     * @param message Exception message
     */
    public DatabaseScannerException(String message) {
        super(message, "SCANNER_ERROR");
    }

    /**
     * Creates a new DatabaseScannerException with the specified message and cause.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     */
    public DatabaseScannerException(String message, Throwable cause) {
        super(message, cause, "SCANNER_ERROR");
    }
}