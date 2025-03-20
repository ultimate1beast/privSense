package com.cgi.privsense.dbscanner.model;

import lombok.*;

import java.io.Serial;

/**
 * Represents the metadata of a database column.
 * Contains all descriptive and structural information of a column.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
public class ColumnMetadata extends BaseMetaData {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * SQL data type of the column (VARCHAR, INT, etc.).
     */
    private String type;

    /**
     * Maximum length for variable-length data types.
     */
    private Long maxLength;

    /**
     * Indicates whether the column can contain NULL values.
     */
    private boolean nullable;

    /**
     * Indicates whether the column is part of the primary key.
     */
    private boolean primaryKey;

    /**
     * Default value of the column.
     */
    private String defaultValue;

    /**
     * Ordinal position of the column in the table.
     */
    private int ordinalPosition;

    /**
     * Name of the table to which this column belongs.
     */
    private String tableName;

    /**
     * Indicates whether the column is a foreign key.
     */
    private boolean foreignKey;

    /**
     * If it's a foreign key, the referenced table.
     */
    private String referencedTable;

    /**
     * If it's a foreign key, the referenced column.
     */
    private String referencedColumn;

    /**
     * Provides a summarized representation of the column.
     *
     * @return A descriptive string of the column
     */
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(" (").append(type);

        if (maxLength != null && maxLength > 0) {
            sb.append("(").append(maxLength).append(")");
        }

        if (primaryKey) {
            sb.append(", PK");
        }

        if (foreignKey) {
            sb.append(", FK -> ").append(referencedTable);
            if (referencedColumn != null) {
                sb.append(".").append(referencedColumn);
            }
        }

        if (!nullable) {
            sb.append(", NOT NULL");
        }

        sb.append(")");
        return sb.toString();
    }
}