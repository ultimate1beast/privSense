package com.cgi.privsense.dbscanner.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for a database relationship.
 */
@Data
public class RelationshipMetadata {
    /**
     * Relationship/constraint name.
     */
    private String name;

    /**
     * Source table name.
     */
    private String sourceTable;

    /**
     * Target table name.
     */
    private String targetTable;

    /**
     * Target schema name.
     */
    private String targetSchema;

    /**
     * Constraint type (e.g., "FOREIGN KEY").
     */
    private String constraintType;

    /**
     * Update rule (e.g., "CASCADE", "RESTRICT").
     */
    private String updateRule;

    /**
     * Delete rule (e.g., "CASCADE", "RESTRICT").
     */
    private String deleteRule;

    /**
     * Relationship direction (INCOMING or OUTGOING).
     */
    private String direction;

    /**
     * Column mappings (source column -> target column).
     */
    private final Map<String, String> columnMappings = new HashMap<>();

    /**
     * Adds a column mapping to the relationship.
     *
     * @param sourceColumn Source column name
     * @param targetColumn Target column name
     */
    public void addColumnMapping(String sourceColumn, String targetColumn) {
        columnMappings.put(sourceColumn, targetColumn);
    }

    /**
     * Gets the list of source columns.
     *
     * @return List of source column names
     */
    public List<String> getSourceColumns() {
        return new ArrayList<>(columnMappings.keySet());
    }

    /**
     * Gets the list of target columns.
     *
     * @return List of target column names
     */
    public List<String> getTargetColumns() {
        return new ArrayList<>(columnMappings.values());
    }
}