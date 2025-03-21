package com.cgi.privsense.dbscanner.service.queue;

import com.cgi.privsense.dbscanner.model.DataSample;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Represents a sampling task for database data.
 * Contains all information needed to sample data from a table or column.
 * Immutable class to ensure thread safety.
 */
public class SamplingTask {
    private final String id;
    private final String connectionId;
    private final String dbType;
    private final String tableName;
    private final String columnName; // null for table sampling
    private final int limit;
    private final Consumer<List<Object>> columnCallback;
    private final Consumer<DataSample> tableCallback;

    /**
     * Constructor for column sampling task.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param columnName Column name
     * @param limit Maximum number of rows to sample
     * @param columnCallback Callback to receive column sampling results
     */
    public SamplingTask(String dbType, String connectionId, String tableName,
                        String columnName, int limit, Consumer<List<Object>> columnCallback) {
        this.id = UUID.randomUUID().toString();
        this.dbType = dbType;
        this.connectionId = connectionId;
        this.tableName = tableName;
        this.columnName = columnName;
        this.limit = limit;
        this.columnCallback = columnCallback;
        this.tableCallback = null;
    }

    /**
     * Constructor for table sampling task.
     *
     * @param dbType Database type
     * @param connectionId Connection ID
     * @param tableName Table name
     * @param limit Maximum number of rows to sample
     * @param tableCallback Callback to receive table sampling results
     */
    public SamplingTask(String dbType, String connectionId, String tableName,
                        int limit, Consumer<DataSample> tableCallback) {
        this.id = UUID.randomUUID().toString();
        this.dbType = dbType;
        this.connectionId = connectionId;
        this.tableName = tableName;
        this.columnName = null;
        this.limit = limit;
        this.columnCallback = null;
        this.tableCallback = tableCallback;
    }

    /**
     * Gets the task ID.
     *
     * @return Task ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the connection ID.
     *
     * @return Connection ID
     */
    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Gets the database type.
     *
     * @return Database type
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * Gets the table name.
     *
     * @return Table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the column name.
     *
     * @return Column name, or null for table sampling
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Gets the sample limit.
     *
     * @return Maximum number of rows to sample
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Gets the column callback.
     *
     * @return Column callback
     */
    public Consumer<List<Object>> getColumnCallback() {
        return columnCallback;
    }

    /**
     * Gets the table callback.
     *
     * @return Table callback
     */
    public Consumer<DataSample> getTableCallback() {
        return tableCallback;
    }

    /**
     * Checks if this task is for sampling a table.
     *
     * @return true if this is a table sampling task
     */
    public boolean isTableSamplingTask() {
        return columnName == null && tableCallback != null;
    }

    /**
     * Checks if this task is for sampling a column.
     *
     * @return true if this is a column sampling task
     */
    public boolean isColumnSamplingTask() {
        return columnName != null && columnCallback != null;
    }

    @Override
    public String toString() {
        if (isColumnSamplingTask()) {
            return "SamplingTask{id='" + id + "', table='" + tableName + "', column='" + columnName + "', limit=" + limit + "}";
        } else {
            return "SamplingTask{id='" + id + "', table='" + tableName + "', limit=" + limit + "}";
        }
    }
}