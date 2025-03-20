package com.cgi.privsense.dbscanner.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for metadata.
 * Provides common properties and functionalities
 * shared by all types of metadata.
 */
@Getter
@Setter
@ToString
public abstract class BaseMetaData implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Name of the metadata object (table, column, etc.).
     */
    protected String name;

    /**
     * Comment or description associated with the object.
     */
    protected String comment;

    /**
     * Additional information that does not fit into standard attributes.
     * Allows extending metadata without modifying the class structure.
     */
    protected Map<String, Object> additionalInfo = new HashMap<>();

    /**
     * Adds additional information to the metadata.
     *
     * @param key   Key of the information
     * @param value Associated value
     */
    public void addAdditionalInfo(String key, Object value) {
        this.additionalInfo.put(key, value);
    }

    /**
     * Retrieves additional information.
     *
     * @param key Key of the information to retrieve
     * @return The associated value or null if not found
     */
    public Object getAdditionalInfoValue(String key) {
        return this.additionalInfo.get(key);
    }
}