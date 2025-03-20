package com.cgi.privsense.dbscanner.exception;

/**
 * Exception for scanner service errors.
 */
public class ScannerException extends BaseException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new ScannerException with the specified message.
     *
     * @param message Exception message
     */
    public ScannerException(String message) {
        super(message, "SCANNER_SERVICE_ERROR");
    }

    /**
     * Creates a new ScannerException with the specified message and cause.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     */
    public ScannerException(String message, Throwable cause) {
        super(message, cause, "SCANNER_SERVICE_ERROR");
    }
}