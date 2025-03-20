package com.cgi.privsense.dbscanner.exception;

/**
 * Exception for sampling-related errors.
 */
public class SamplingException extends BaseException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new SamplingException with the specified message.
     *
     * @param message Exception message
     */
    public SamplingException(String message) {
        super(message, "SAMPLING_ERROR");
    }

    /**
     * Creates a new SamplingException with the specified message and cause.
     *
     * @param message Exception message
     * @param cause The cause of the exception
     */
    public SamplingException(String message, Throwable cause) {
        super(message, cause, "SAMPLING_ERROR");
    }
}