package com.cgi.privsense.dbscanner.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the metadata of a database table.
 * Encapsulates all structural information of a table, including its columns.
 */
@Getter
@Setter
@ToString(callSuper = true)
public class TableMetadata extends BaseMetaData {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Catalog to which the table belongs.
     */
    private String catalog;

    /**
     * Schema to which the table belongs.
     */
    private String schema;

    /**
     * Table type (TABLE, VIEW, etc.).
     */
    private String type;

    /**
     * Approximate number of rows in the table.
     */
    private Long approximateRowCount;

    /**
     * List of columns in the table.
     */
    private List<ColumnMetadata> columns = new ArrayList<>();

    /**
     * Additional information about the table.
     */
    private Map<String, Object> additionalInfo = new HashMap<>();

    /**
     * Default constructor.
     */
    public TableMetadata() {
        this.columns = new ArrayList<>();
    }

    /**
     * Constructor with table name.
     *
     * @param name Table name
     */
    public TableMetadata(String name) {
        this();
        this.name = name;
    }

    /**
     * Constructor with all basic properties.
     *
     * @param name Table name
     * @param catalog Catalog name
     * @param schema Schema name
     * @param type Table type
     * @param comment Table comment
     */
    public TableMetadata(String name, String catalog, String schema, String type, String comment) {
        this();
        this.name = name;
        this.catalog = catalog;
        this.schema = schema;
        this.type = type;
        this.comment = comment;
    }

    /**
     * Adds a column to the table.
     *
     * @param column Column metadata
     */
    public void addColumn(ColumnMetadata column) {
        this.columns.add(column);
    }
}