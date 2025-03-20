package com.cgi.privsense.dbscanner.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Request for sampling data.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SamplingRequest {

    @Schema(description = "List of items to sample (table names or column names)")
    private List<String> items = new ArrayList<>();

    /**
     * Validates the sampling request to ensure all items are valid.
     * This method can be called before processing the request.
     *
     * @return True if the request is valid, false otherwise
     */
    public boolean isValid() {
        if (items == null || items.isEmpty()) {
            return false;
        }

        // Check each item is not null or empty
        for (String item : items) {
            if (item == null || item.trim().isEmpty()) {
                return false;
            }
        }

        return true;
    }
}