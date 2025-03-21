package com.cgi.privsense.dbscanner.api;

import com.cgi.privsense.dbscanner.api.dto.ApiResponse;
import com.cgi.privsense.dbscanner.api.dto.ConnectionRequest;
import com.cgi.privsense.dbscanner.api.dto.SamplingRequest;
import com.cgi.privsense.dbscanner.core.datasource.DataSourceProvider;
import com.cgi.privsense.dbscanner.model.ColumnMetadata;
import com.cgi.privsense.dbscanner.model.DataSample;
import com.cgi.privsense.dbscanner.model.RelationshipMetadata;
import com.cgi.privsense.dbscanner.model.TableMetadata;
import com.cgi.privsense.dbscanner.service.OptimizedParallelSamplingService;
import com.cgi.privsense.dbscanner.service.ScannerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Controller for database scanning and sampling operations.
 */
@RestController
@RequestMapping("/api/scanner")
@Tag(name = "Database Scanner", description = "API for scanning and sampling database structures and data")
public class DatabaseScannerController {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseScannerController.class);

    /**
     * Scanner service.
     */
    private final ScannerService scannerService;

    /**
     * Parallel sampling service.
     */
    private final OptimizedParallelSamplingService samplingService;

    /**
     * Data source provider.
     */
    private final DataSourceProvider dataSourceProvider;

    /**
     * Constructor.
     *
     * @param scannerService Scanner service
     * @param samplingService Parallel sampling service
     * @param dataSourceProvider Data source provider
     */
    public DatabaseScannerController(
            ScannerService scannerService,
            OptimizedParallelSamplingService samplingService,
            DataSourceProvider dataSourceProvider) {
        this.scannerService = scannerService;
        this.samplingService = samplingService;
        this.dataSourceProvider = dataSourceProvider;
    }


    @Operation(summary = "Register a new connection")
    @PostMapping("/connections")
    public ResponseEntity<ApiResponse<Map<String, String>>> registerConnection(@RequestBody ConnectionRequest request) {
        // Generate connection ID if not provided
        String connectionId = request.generateConnectionId();

        // Set the generated ID back to the request
        request.setName(connectionId);

        logger.info("Registering connection with ID: {}", connectionId);

        // Create and register the data source
        DataSource dataSource = dataSourceProvider.createDataSource(request.toConnectionRequest());
        dataSourceProvider.registerDataSource(connectionId, dataSource);

        // Return success with the generated connection ID
        Map<String, String> response = new HashMap<>();
        response.put("connectionId", connectionId);
        response.put("message", "Connection registered successfully");

        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @Operation(summary = "Delete an existing connection")
    @DeleteMapping("/connections/{connectionId}")
    public ResponseEntity<ApiResponse<String>> deleteConnection(@PathVariable String connectionId) {
        logger.info("Deleting connection: {}", connectionId);

        // Validate that the connection exists
        if (!dataSourceProvider.hasDataSource(connectionId)) {
            logger.warn("Connection not found: {}", connectionId);
            return ResponseEntity
                    .status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.error("Connection not found: " + connectionId));
        }

        // Remove the connection
        boolean success = dataSourceProvider.removeDataSource(connectionId);

        if (success) {
            logger.info("Connection deleted successfully: {}", connectionId);
            return ResponseEntity.ok(ApiResponse.success("Connection deleted: " + connectionId));
        } else {
            logger.error("Failed to delete connection: {}", connectionId);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to delete connection: " + connectionId));
        }
    }

    @Operation(summary = "List all active connections")
    @GetMapping("/connections")
    public ResponseEntity<ApiResponse<List<Map<String, Object>>>> listConnections() {
        logger.info("Listing all connections");

        List<Map<String, Object>> connections = dataSourceProvider.getDataSourcesInfo();
        return ResponseEntity.ok(ApiResponse.success(connections));
    }


    // === Scanning operations ===

    @Operation(summary = "Scan all tables")
    @GetMapping("/connections/{connectionId}/tables")
    public ResponseEntity<ApiResponse<List<TableMetadata>>> scanTables(
            @PathVariable String connectionId,
            @RequestParam(required = false) String dbType) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Scanning tables for connection: {}, db type: {}", connectionId, actualDbType);
        List<TableMetadata> tables = scannerService.scanTables(actualDbType, connectionId);
        return ResponseEntity.ok(ApiResponse.success(tables));
    }

    @Operation(summary = "Scan a specific table")
    @GetMapping("/connections/{connectionId}/tables/{tableName}")
    public ResponseEntity<ApiResponse<TableMetadata>> scanTable(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @RequestParam(required = false) String dbType) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Scanning table: {}, connection: {}, db type: {}", tableName, connectionId, actualDbType);
        TableMetadata table = scannerService.scanTable(actualDbType, connectionId, tableName);
        return ResponseEntity.ok(ApiResponse.success(table));
    }

    @Operation(summary = "Scan columns of a table")
    @GetMapping("/connections/{connectionId}/tables/{tableName}/columns")
    public ResponseEntity<ApiResponse<List<ColumnMetadata>>> scanColumns(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @RequestParam(required = false) String dbType) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Scanning columns for table: {}, connection: {}, db type: {}",
                tableName, connectionId, actualDbType);
        List<ColumnMetadata> columns = scannerService.scanColumns(actualDbType, connectionId, tableName);
        return ResponseEntity.ok(ApiResponse.success(columns));
    }

    // === Sampling operations ===

    @Operation(summary = "Sample data from a table")
    @GetMapping("/connections/{connectionId}/tables/{tableName}/sample")
    public ResponseEntity<ApiResponse<DataSample>> sampleData(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) String dbType) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Sampling data from table: {}, connection: {}, db type: {}, limit: {}",
                tableName, connectionId, actualDbType, limit);
        DataSample sample = samplingService.sampleTable(actualDbType, connectionId, tableName, limit);
        return ResponseEntity.ok(ApiResponse.success(sample));
    }

    @Operation(summary = "Sample data from a column")
    @GetMapping("/connections/{connectionId}/tables/{tableName}/columns/{columnName}/sample")
    public ResponseEntity<ApiResponse<List<Object>>> sampleColumnData(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) String dbType) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Sampling data from column: {}.{}, connection: {}, db type: {}, limit: {}",
                tableName, columnName, connectionId, actualDbType, limit);
        List<Object> samples = samplingService.sampleColumn(actualDbType, connectionId, tableName, columnName, limit);
        return ResponseEntity.ok(ApiResponse.success(samples));
    }

    @Operation(summary = "Sample data from multiple columns in parallel")
    @PostMapping("/connections/{connectionId}/tables/{tableName}/columns/sample")
    public ResponseEntity<ApiResponse<Map<String, List<Object>>>> sampleColumnsInParallel(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "10") int limit,
            @RequestBody SamplingRequest request) {

        // Validate the request
        if (request == null || request.getItems() == null || request.getItems().isEmpty()) {
            return ResponseEntity
                    .badRequest()
                    .body(ApiResponse.error("No columns specified for sampling"));
        }

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Sampling multiple columns in parallel from table: {}, connection: {}, db type: {}, limit: {}, columns: {}",
                tableName, connectionId, actualDbType, limit, String.join(", ", request.getItems()));

        Map<String, List<Object>> samples = samplingService.sampleColumnsInParallel(
                actualDbType, connectionId, tableName, request.getItems(), limit);

        if (samples.isEmpty()) {
            // Create an empty map of the correct type
            Map<String, List<Object>> emptyResult = new HashMap<>();
            // Add a message to the logs, but return the empty map
            logger.warn("None of the requested columns could be found in table {}", tableName);
            return ResponseEntity.ok(ApiResponse.success(emptyResult));
        }

        return ResponseEntity.ok(ApiResponse.success(samples));
    }

    @Operation(summary = "Sample data from multiple tables in parallel")
    @PostMapping("/connections/{connectionId}/tables/sample")
    public ResponseEntity<ApiResponse<Map<String, DataSample>>> sampleTablesInParallel(
            @PathVariable String connectionId,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "10") int limit,
            @RequestBody SamplingRequest request) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Sampling multiple tables in parallel, connection: {}, db type: {}, limit: {}",
                connectionId, actualDbType, limit);
        Map<String, DataSample> samples = samplingService.sampleTablesInParallel(
                actualDbType, connectionId, request.getItems(), limit);
        return ResponseEntity.ok(ApiResponse.success(samples));
    }

    // === Relationship operations ===

    @Operation(summary = "Get detailed relationships of a table")
    @GetMapping("/connections/{connectionId}/tables/{tableName}/relationships")
    public ResponseEntity<ApiResponse<List<RelationshipMetadata>>> getTableRelationships(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @RequestParam(required = false) String dbType) {

        String actualDbType = resolveDbType(connectionId, dbType);
        logger.info("Getting detailed relationships for table: {}, connection: {}, db type: {}",
                tableName, connectionId, actualDbType);
        List<RelationshipMetadata> relationships = scannerService.getTableRelationships(actualDbType, connectionId, tableName);
        return ResponseEntity.ok(ApiResponse.success(relationships));
    }

    // === Utility methods ===

    /**
     * Resolves the database type if not specified.
     *
     * @param connectionId Connection ID
     * @param dbType Database type (optional)
     * @return Resolved database type
     */
    private String resolveDbType(String connectionId, String dbType) {
        if (dbType != null && !dbType.isEmpty()) {
            return dbType;
        }

        return dataSourceProvider.getDatabaseType(connectionId);
    }
}