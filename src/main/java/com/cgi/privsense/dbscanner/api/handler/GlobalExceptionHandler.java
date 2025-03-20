package com.cgi.privsense.dbscanner.api.handler;

import com.cgi.privsense.dbscanner.api.dto.ApiResponse;
import com.cgi.privsense.dbscanner.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * Global exception handler for the application.
 * Handles all exceptions and returns appropriate API responses.
 */
@ControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles DataSourceException.
     *
     * @param e DataSourceException
     * @return API response with error message
     */
    @ExceptionHandler(DataSourceException.class)
    public ResponseEntity<ApiResponse<String>> handleDataSourceException(DataSourceException e) {
        logger.error("Data source error: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(e.getMessage(), e.getErrorCode()));
    }

    /**
     * Handles DriverException.
     *
     * @param e DriverException
     * @return API response with error message
     */
    @ExceptionHandler(DriverException.class)
    public ResponseEntity<ApiResponse<String>> handleDriverException(DriverException e) {
        logger.error("Driver error: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(e.getMessage(), e.getErrorCode()));
    }

    /**
     * Handles DatabaseScannerException.
     *
     * @param e DatabaseScannerException
     * @return API response with error message
     */
    @ExceptionHandler(DatabaseScannerException.class)
    public ResponseEntity<ApiResponse<String>> handleDatabaseScannerException(DatabaseScannerException e) {
        logger.error("Database scanner error: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(e.getMessage(), e.getErrorCode()));
    }

    /**
     * Handles ScannerException.
     *
     * @param e ScannerException
     * @return API response with error message
     */
    @ExceptionHandler(ScannerException.class)
    public ResponseEntity<ApiResponse<String>> handleScannerException(ScannerException e) {
        logger.error("Scanner service error: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(e.getMessage(), e.getErrorCode()));
    }

    /**
     * Handles SamplingException.
     *
     * @param e SamplingException
     * @return API response with error message
     */
    @ExceptionHandler(SamplingException.class)
    public ResponseEntity<ApiResponse<String>> handleSamplingException(SamplingException e) {
        logger.error("Sampling error: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(e.getMessage(), e.getErrorCode()));
    }

    /**
     * Handles all other exceptions.
     *
     * @param e Exception
     * @return API response with error message
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<String>> handleGenericException(Exception e) {
        logger.error("Unexpected error: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("An unexpected error occurred: " + e.getMessage(), "GENERAL_ERROR"));
    }

    /**
     * Handles IllegalArgumentException.
     *
     * @param e IllegalArgumentException
     * @return API response with error message
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<String>> handleIllegalArgumentException(IllegalArgumentException e) {
        logger.error("Invalid argument: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.error("Invalid argument: " + e.getMessage(), "INVALID_ARGUMENT"));
    }
}