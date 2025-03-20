package com.cgi.privsense.dbscanner.exception;

/**
 * Exception for driver-related errors.
 */
public class DriverException extends BaseException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new DriverException with the specified message.
     *
     * @param message Exception message
     */
    public DriverException(String message) {
        super(message, "DRIVER_ERROR");
    }

    /**
     * Creates a new DriverException with the specified message and cause.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     */
    public DriverException(String message, Throwable cause) {
        super(message, cause, "DRIVER_ERROR");
    }
}