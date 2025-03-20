package com.cgi.privsense.dbscanner.scanner;

import com.cgi.privsense.dbscanner.core.scanner.AbstractDatabaseScanner;

import com.cgi.privsense.dbscanner.core.scanner.DatabaseType;
import com.cgi.privsense.dbscanner.exception.DatabaseScannerException;
import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.model.RelationshipMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Scanner implementation for MySQL databases.
 */
@Component
@DatabaseType("mysql")
public class MySQLDatabaseScanner extends AbstractDatabaseScanner {
    // SQL constants for reuse
    private static final String SQL_SCAN_TABLES = """
        SELECT 
            t.TABLE_NAME,
            t.TABLE_SCHEMA as TABLE_CATALOG,
            t.TABLE_SCHEMA as `SCHEMA`,
            t.TABLE_TYPE as TYPE,
            t.TABLE_COMMENT as REMARKS,
            t.TABLE_ROWS as ROW_COUNT,
            t.AUTO_INCREMENT,
            t.CREATE_TIME,
            t.UPDATE_TIME,
            t.ENGINE,
            GROUP_CONCAT(DISTINCT c.COLUMN_NAME) as COLUMNS,
            GROUP_CONCAT(DISTINCT k.REFERENCED_TABLE_NAME) as REFERENCED_TABLES
        FROM information_schema.TABLES t
        LEFT JOIN information_schema.COLUMNS c 
            ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
            AND t.TABLE_NAME = c.TABLE_NAME
        LEFT JOIN information_schema.KEY_COLUMN_USAGE k
            ON t.TABLE_SCHEMA = k.TABLE_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            AND k.REFERENCED_TABLE_NAME IS NOT NULL
        WHERE t.TABLE_SCHEMA = database()
        GROUP BY t.TABLE_NAME, t.TABLE_SCHEMA, t.TABLE_TYPE, 
                t.TABLE_COMMENT, t.TABLE_ROWS, t.AUTO_INCREMENT,
                t.CREATE_TIME, t.UPDATE_TIME, t.ENGINE
    """;

    private static final String SQL_SCAN_COLUMNS = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.COLUMN_COMMENT,
            c.IS_NULLABLE,
            c.COLUMN_KEY,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION,
            CASE 
                WHEN k.REFERENCED_TABLE_NAME IS NOT NULL THEN true 
                ELSE false 
            END as IS_FOREIGN_KEY,
            k.REFERENCED_TABLE_NAME,
            k.REFERENCED_COLUMN_NAME
        FROM information_schema.COLUMNS c
        LEFT JOIN information_schema.KEY_COLUMN_USAGE k 
            ON c.TABLE_SCHEMA = k.TABLE_SCHEMA 
            AND c.TABLE_NAME = k.TABLE_NAME 
            AND c.COLUMN_NAME = k.COLUMN_NAME 
            AND k.REFERENCED_TABLE_NAME IS NOT NULL
        WHERE c.TABLE_SCHEMA = database() 
        AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """;

    private static final String SQL_GET_RELATIONSHIPS = """
        SELECT 
            tc.CONSTRAINT_NAME,
            tc.CONSTRAINT_TYPE,
            tc.TABLE_NAME as SOURCE_TABLE,
            kcu.REFERENCED_TABLE_NAME as TARGET_TABLE,
            kcu.REFERENCED_TABLE_SCHEMA as TARGET_SCHEMA,
            rc.UPDATE_RULE,
            rc.DELETE_RULE,
            kcu.COLUMN_NAME as SOURCE_COLUMN,
            kcu.REFERENCED_COLUMN_NAME as TARGET_COLUMN
        FROM information_schema.TABLE_CONSTRAINTS tc
        JOIN information_schema.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
            ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        WHERE tc.TABLE_SCHEMA = database()
        AND tc.TABLE_NAME = ?
        AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
    """;

    /**
     * Constructor.
     *
     * @param dataSource The data source
     */
    public MySQLDatabaseScanner(DataSource dataSource) {
        super(dataSource, "mysql");
    }

    /**
     * Scans all tables in the database.
     *
     * @return List of table metadata
     */
    @Override
    public List<TableMetadata> scanTables() {
        return executeQuery("scanTables", jdbc -> jdbc.query(SQL_SCAN_TABLES, this::mapTableMetadata));
    }

    /**
     * Maps a result set row to table metadata.
     *
     * @param rs Result set
     * @param rowNum Row number
     * @return Table metadata
     * @throws SQLException On SQL error
     */
    private TableMetadata mapTableMetadata(ResultSet rs, int rowNum) throws SQLException {
        TableMetadata metadata = new TableMetadata();
        metadata.setName(rs.getString("TABLE_NAME"));
        metadata.setCatalog(rs.getString("TABLE_CATALOG"));
        metadata.setSchema(rs.getString("SCHEMA"));
        metadata.setType(rs.getString("TYPE"));
        metadata.setComment(rs.getString("REMARKS"));
        metadata.setApproximateRowCount(rs.getLong("ROW_COUNT"));

        // Additional metadata
        Map<String, Object> additionalInfo = new HashMap<>();
        additionalInfo.put("engine", rs.getString("ENGINE"));
        additionalInfo.put("autoIncrement", rs.getLong("AUTO_INCREMENT"));
        additionalInfo.put("createTime", rs.getTimestamp("CREATE_TIME"));
        additionalInfo.put("updateTime", rs.getTimestamp("UPDATE_TIME"));
        additionalInfo.put("referencedTables",
                Optional.ofNullable(rs.getString("REFERENCED_TABLES"))
                        .map(s -> Arrays.asList(s.split(",")))
                        .orElse(Collections.emptyList()));

        metadata.setAdditionalInfo(additionalInfo);

        // Lazy loading columns when requested separately
        // metadata.setColumns(scanColumns(metadata.getName()));

        return metadata;
    }

    /**
     * Scans columns of a table.
     *
     * @param tableName Table name
     * @return List of column metadata
     */
    @Override
    public List<ColumnMetadata> scanColumns(String tableName) {
        validateTableName(tableName);
        return executeQuery("scanColumns", jdbc ->
                jdbc.query(SQL_SCAN_COLUMNS, (rs, rowNum) -> mapColumnMetadata(rs, tableName), tableName)
        );
    }

    /**
     * Maps a result set row to column metadata.
     *
     * @param rs Result set
     * @param tableName Table name
     * @return Column metadata
     * @throws SQLException On SQL error
     */
    private ColumnMetadata mapColumnMetadata(ResultSet rs, String tableName) throws SQLException {
        ColumnMetadata metadata = new ColumnMetadata();
        metadata.setName(rs.getString("COLUMN_NAME"));
        metadata.setType(rs.getString("DATA_TYPE"));
        metadata.setMaxLength(rs.getLong("CHARACTER_MAXIMUM_LENGTH"));
        metadata.setComment(rs.getString("COLUMN_COMMENT"));
        metadata.setNullable("YES".equals(rs.getString("IS_NULLABLE")));
        metadata.setPrimaryKey("PRI".equals(rs.getString("COLUMN_KEY")));
        metadata.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
        metadata.setOrdinalPosition(rs.getInt("ORDINAL_POSITION"));
        metadata.setTableName(tableName);
        metadata.setForeignKey(rs.getBoolean("IS_FOREIGN_KEY"));
        metadata.setReferencedTable(rs.getString("REFERENCED_TABLE_NAME"));
        metadata.setReferencedColumn(rs.getString("REFERENCED_COLUMN_NAME"));
        return metadata;
    }

    /**
     * Scans a specific table.
     *
     * @param tableName Table name
     * @return Table metadata
     */
    @Override
    public TableMetadata scanTable(String tableName) {
        validateTableName(tableName);
        return executeQuery("scanTable", jdbc -> {
            // Modify the SQL query to properly place the table name filter in the WHERE clause
            String modifiedSql = SQL_SCAN_TABLES.replace(
                    "WHERE t.TABLE_SCHEMA = database()",
                    "WHERE t.TABLE_SCHEMA = database() AND t.TABLE_NAME = ?"
            );

            List<TableMetadata> tables = jdbc.query(
                    modifiedSql,
                    this::mapTableMetadata,
                    tableName
            );

            if (tables.isEmpty()) {
                throw new DatabaseScannerException("Table not found: " + tableName);
            }

            TableMetadata table = tables.getFirst();
            table.setColumns(scanColumns(tableName));
            return table;
        });
    }

    /**
     * Samples data from a table.
     *
     * @param tableName Table name
     * @param limit Maximum number of rows
     * @return Data sample
     */
    @Override
    public DataSample sampleTableData(String tableName, int limit) {
        validateTableName(tableName);
        return executeQuery("sampleTableData", jdbc -> {
            String sql = String.format("SELECT * FROM %s LIMIT ?", escapeIdentifier(tableName));
            List<Map<String, Object>> rows = jdbc.queryForList(sql, limit);
            return DataSample.fromRows(tableName, rows);
        });
    }

    /**
     * Samples data from a column.
     *
     * @param tableName Table name
     * @param columnName Column name
     * @param limit Maximum number of values
     * @return List of sampled values
     */
    @Override
    public List<Object> sampleColumnData(String tableName, String columnName, int limit) {
        validateTableName(tableName);
        validateColumnName(columnName);

        String sql = String.format("SELECT %s FROM %s LIMIT ?",
                escapeIdentifier(columnName),
                escapeIdentifier(tableName));

        return executeQuery("sampleColumnData", jdbc ->
                jdbc.queryForList(sql, Object.class, limit)
        );
    }

    /**
     * Gets detailed relationships for a table, including both outgoing and incoming relationships.
     *
     * @param tableName Table name
     * @return List of relationship metadata
     */
    @Override
    public List<RelationshipMetadata> getTableRelationships(String tableName) {
        validateTableName(tableName);
        logger.info("Retrieving relationships for table: {}", tableName);

        return executeQuery("getTableRelationships", jdbc -> {
            Map<String, RelationshipMetadata> relationshipsMap = new HashMap<>();

            // SQL for outgoing relationships (FKs defined on this table)
            String outgoingRelationshipsSql = """
            SELECT 
                tc.CONSTRAINT_NAME,
                'OUTGOING' as RELATIONSHIP_DIRECTION,
                tc.CONSTRAINT_TYPE,
                tc.TABLE_NAME as SOURCE_TABLE,
                kcu.REFERENCED_TABLE_NAME as TARGET_TABLE,
                kcu.REFERENCED_TABLE_SCHEMA as TARGET_SCHEMA,
                rc.UPDATE_RULE,
                rc.DELETE_RULE,
                kcu.COLUMN_NAME as SOURCE_COLUMN,
                kcu.REFERENCED_COLUMN_NAME as TARGET_COLUMN
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = database()
            AND tc.TABLE_NAME = ?
            AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        """;

            // SQL for incoming relationships (FKs that reference this table) - FIXED THE = SIGN
            String incomingRelationshipsSql = """
            SELECT 
                tc.CONSTRAINT_NAME,
                'INCOMING' as RELATIONSHIP_DIRECTION,
                tc.CONSTRAINT_TYPE,
                tc.TABLE_NAME as SOURCE_TABLE,
                ? as TARGET_TABLE,
                database() as TARGET_SCHEMA,
                rc.UPDATE_RULE,
                rc.DELETE_RULE,
                kcu.COLUMN_NAME as SOURCE_COLUMN,
                kcu.REFERENCED_COLUMN_NAME as TARGET_COLUMN
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
                AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = database()
            AND kcu.REFERENCED_TABLE_SCHEMA = database()
            AND kcu.REFERENCED_TABLE_NAME = ?
            AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        """;

            // Process outgoing relationships
            List<Map<String, Object>> outgoingResults = jdbc.queryForList(outgoingRelationshipsSql, tableName);
            logger.info("Found {} outgoing relationships", outgoingResults.size());

            for (Map<String, Object> row : outgoingResults) {
                String constraintName = (String) row.get("CONSTRAINT_NAME");
                String directionKey = "OUTGOING_" + constraintName;

                logger.info("Processing outgoing relationship: {}, source: {}, target: {}",
                        constraintName, row.get("SOURCE_TABLE"), row.get("TARGET_TABLE"));

                RelationshipMetadata rel = new RelationshipMetadata();
                rel.setName(constraintName);
                rel.setConstraintType((String) row.get("CONSTRAINT_TYPE"));
                rel.setSourceTable((String) row.get("SOURCE_TABLE"));
                rel.setTargetTable((String) row.get("TARGET_TABLE"));
                rel.setTargetSchema((String) row.get("TARGET_SCHEMA"));
                rel.setUpdateRule((String) row.get("UPDATE_RULE"));
                rel.setDeleteRule((String) row.get("DELETE_RULE"));
                rel.setDirection("OUTGOING");

                rel.addColumnMapping(
                        (String) row.get("SOURCE_COLUMN"),
                        (String) row.get("TARGET_COLUMN")
                );

                relationshipsMap.put(directionKey, rel);
            }

            // Process incoming relationships
            List<Map<String, Object>> incomingResults = jdbc.queryForList(incomingRelationshipsSql, tableName, tableName);
            logger.info("Found {} incoming relationships", incomingResults.size());

            for (Map<String, Object> row : incomingResults) {
                String constraintName = (String) row.get("CONSTRAINT_NAME");
                String directionKey = "INCOMING_" + constraintName;

                logger.info("Processing incoming relationship: {}, source: {}, target: {}",
                        constraintName, row.get("SOURCE_TABLE"), row.get("TARGET_TABLE"));

                RelationshipMetadata rel = new RelationshipMetadata();
                rel.setName(constraintName);
                rel.setConstraintType((String) row.get("CONSTRAINT_TYPE"));
                rel.setSourceTable((String) row.get("SOURCE_TABLE"));
                rel.setTargetTable((String) row.get("TARGET_TABLE"));
                rel.setTargetSchema((String) row.get("TARGET_SCHEMA"));
                rel.setUpdateRule((String) row.get("UPDATE_RULE"));
                rel.setDeleteRule((String) row.get("DELETE_RULE"));
                rel.setDirection("INCOMING");

                rel.addColumnMapping(
                        (String) row.get("SOURCE_COLUMN"),
                        (String) row.get("TARGET_COLUMN")
                );

                relationshipsMap.put(directionKey, rel);
            }

            logger.info("Total relationships found: {}", relationshipsMap.size());
            return new ArrayList<>(relationshipsMap.values());
        });
    }

    /**
     * Helper method to process relationship query results.
     *
     * @param rs Result set containing relationship data
     * @param relationshipsMap Map to store relationship objects
     * @throws SQLException On SQL error
     */
    private void processRelationships(ResultSet rs, Map<String, RelationshipMetadata> relationshipsMap) throws SQLException {
        while (rs.next()) {
            String constraintName = rs.getString("CONSTRAINT_NAME");
            String directionKey = rs.getString("RELATIONSHIP_DIRECTION") + "_" + constraintName;

            // Check if relationship exists already
            RelationshipMetadata relationship;
            if (!relationshipsMap.containsKey(directionKey)) {
                // Create new relationship
                RelationshipMetadata rel = new RelationshipMetadata();
                rel.setName(constraintName);
                rel.setConstraintType(rs.getString("CONSTRAINT_TYPE"));
                rel.setSourceTable(rs.getString("SOURCE_TABLE"));
                rel.setTargetTable(rs.getString("TARGET_TABLE"));
                rel.setTargetSchema(rs.getString("TARGET_SCHEMA"));
                rel.setUpdateRule(rs.getString("UPDATE_RULE"));
                rel.setDeleteRule(rs.getString("DELETE_RULE"));
                rel.setDirection(rs.getString("RELATIONSHIP_DIRECTION"));
                relationshipsMap.put(directionKey, rel);
                relationship = rel;
            } else {
                // Get existing relationship
                relationship = relationshipsMap.get(directionKey);
            }

            // Add the column mapping
            relationship.addColumnMapping(
                    rs.getString("SOURCE_COLUMN"),
                    rs.getString("TARGET_COLUMN")
            );
        }
    }

    /**
     * Escapes an SQL identifier to prevent SQL injection.
     *
     * @param identifier Identifier to escape
     * @return Escaped identifier
     */
    @Override
    protected String escapeIdentifier(String identifier) {
        // MySQL uses backticks for identifiers
        return "`" + identifier.replace("`", "``") + "`";
    }
}