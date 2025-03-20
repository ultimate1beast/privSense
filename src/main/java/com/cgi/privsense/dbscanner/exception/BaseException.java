package com.cgi.privsense.dbscanner.exception;

/**
 * Base exception class for all exceptions in the application.
 * Provides common functionality and standardizes exception handling.
 */
public abstract class BaseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Error code for categorizing exceptions.
     */
    private final String errorCode;

    /**
     * Creates a new BaseException with the specified message and default error code.
     *
     * @param message Exception message
     */
    protected BaseException(String message) {
        this(message, null, "GENERAL_ERROR");
    }

    /**
     * Creates a new BaseException with the specified message and error code.
     *
     * @param message Exception message
     * @param errorCode Error code
     */
    protected BaseException(String message, String errorCode) {
        this(message, null, errorCode);
    }

    /**
     * Creates a new BaseException with the specified message, cause, and default error code.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     */
    protected BaseException(String message, Throwable cause) {
        this(message, cause, "GENERAL_ERROR");
    }

    /**
     * Creates a new BaseException with the specified message, cause, and error code.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     * @param errorCode Error code
     */
    protected BaseException(String message, Throwable cause, String errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Gets the error code.
     *
     * @return Error code
     */
    public String getErrorCode() {
        return errorCode;
    }
}