package com.cgi.privsense.dbscanner.api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Standard API response wrapper.
 *
 * @param <T> Type of data contained in the response
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApiResponse<T> {
    /**
     * Whether the request was successful.
     */
    private boolean success;

    /**
     * Response data.
     */
    private T data;

    /**
     * Error message in case of failure.
     */
    private String error;

    /**
     * Error code in case of failure.
     */
    private String errorCode;

    /**
     * Creates a successful response with data.
     *
     * @param data Response data
     * @param <T> Type of data
     * @return Successful API response
     */
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(true, data, null, null);
    }

    /**
     * Creates an error response with a message.
     *
     * @param errorMessage Error message
     * @param <T> Type of data
     * @return Error API response
     */
    public static <T> ApiResponse<T> error(String errorMessage) {
        return new ApiResponse<>(false, null, errorMessage, "GENERAL_ERROR");
    }

    /**
     * Creates an error response with a message and code.
     *
     * @param errorMessage Error message
     * @param errorCode Error code
     * @param <T> Type of data
     * @return Error API response
     */
    public static <T> ApiResponse<T> error(String errorMessage, String errorCode) {
        return new ApiResponse<>(false, null, errorMessage, errorCode);
    }


}