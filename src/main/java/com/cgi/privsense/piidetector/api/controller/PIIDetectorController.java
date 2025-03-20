package com.cgi.privsense.piidetector.api.controller;

import com.cgi.privsense.dbscanner.api.dto.ApiResponse;
import com.cgi.privsense.piidetector.api.PIIDetector;
import com.cgi.privsense.piidetector.api.PIIReportGenerator;
import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.PIIDetectionResult;
import com.cgi.privsense.piidetector.model.TablePIIInfo;
import com.cgi.privsense.piidetector.service.PIIDetectorImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST Controller for PII detection.
 */
@RestController
@RequestMapping("/api/pii")
@Tag(name = "PII Detector", description = "API for detecting Personally Identifiable Information (PII)")
public class PIIDetectorController {
    private static final Logger log = LoggerFactory.getLogger(PIIDetectorController.class);

    private final PIIDetector piiDetector;
    private final PIIReportGenerator reportGenerator;

    @Autowired
    public PIIDetectorController(PIIDetector piiDetector, PIIReportGenerator reportGenerator) {
        this.piiDetector = piiDetector;
        this.reportGenerator = reportGenerator;
    }

    @Operation(summary = "Analyze a complete database to detect PII")
    @GetMapping("/connections/{connectionId}/scan")
    public ResponseEntity<ApiResponse<PIIDetectionResult>> scanDatabase(
            @PathVariable String connectionId,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "0.7") double confidenceThreshold) {

        log.info("Starting PII analysis for connection: {}, threshold: {}", connectionId, confidenceThreshold);

        piiDetector.setConfidenceThreshold(confidenceThreshold);
        PIIDetectionResult result = piiDetector.detectPII(connectionId, dbType);

        log.info("PII analysis completed for connection: {}. Number of PIIs detected: {}",
                connectionId, result.getTotalPiiCount());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @Operation(summary = "Quickly analyze a database (optimized for performance)")
    @GetMapping("/connections/{connectionId}/quick-scan")
    public ResponseEntity<ApiResponse<PIIDetectionResult>> quickScanDatabase(
            @PathVariable String connectionId,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "0.8") double confidenceThreshold,
            @RequestParam(defaultValue = "5") int sampleSize) {

        log.info("Starting quick PII analysis for connection: {}, threshold: {}, sampleSize: {}",
                connectionId, confidenceThreshold, sampleSize);

        // Configure for quick scan with reduced sample size and higher threshold
        piiDetector.setConfidenceThreshold(confidenceThreshold);

        if (piiDetector instanceof PIIDetectorImpl) {
            ((PIIDetectorImpl) piiDetector).setSampleSize(sampleSize);
        }

        PIIDetectionResult result = piiDetector.detectPII(connectionId, dbType);

        log.info("Quick PII analysis completed for connection: {}. Number of PIIs detected: {}",
                connectionId, result.getTotalPiiCount());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @Operation(summary = "Analyze a specific table to detect PII")
    @GetMapping("/connections/{connectionId}/tables/{tableName}/scan")
    public ResponseEntity<ApiResponse<TablePIIInfo>> scanTable(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "0.7") double confidenceThreshold) {

        log.info("Starting PII analysis for table: {}.{}, threshold: {}",
                connectionId, tableName, confidenceThreshold);

        piiDetector.setConfidenceThreshold(confidenceThreshold);
        TablePIIInfo result = piiDetector.detectPIIInTable(connectionId, dbType, tableName);

        log.info("PII analysis completed for table: {}.{}. PII detected: {}",
                connectionId, tableName, result.isHasPii());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @Operation(summary = "Analyze a specific column to detect PII")
    @GetMapping("/connections/{connectionId}/tables/{tableName}/columns/{columnName}/scan")
    public ResponseEntity<ApiResponse<ColumnPIIInfo>> scanColumn(
            @PathVariable String connectionId,
            @PathVariable String tableName,
            @PathVariable String columnName,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "0.7") double confidenceThreshold) {

        log.info("Starting PII analysis for column: {}.{}.{}, threshold: {}",
                connectionId, tableName, columnName, confidenceThreshold);

        piiDetector.setConfidenceThreshold(confidenceThreshold);
        ColumnPIIInfo result = piiDetector.detectPIIInColumn(connectionId, dbType, tableName, columnName);

        log.info("PII analysis completed for column: {}.{}.{}. PII detected: {}",
                connectionId, tableName, columnName, result.isPiiDetected());

        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @Operation(summary = "Configure PII detection strategies")
    @PostMapping("/configure")
    public ResponseEntity<ApiResponse<String>> configureStrategies(
            @RequestBody Map<String, Boolean> strategies,
            @RequestParam(defaultValue = "0.7") double confidenceThreshold) {

        log.info("Configuring PII strategies: {}, threshold: {}", strategies, confidenceThreshold);

        piiDetector.setConfidenceThreshold(confidenceThreshold);
        piiDetector.configureStrategies(strategies);

        return ResponseEntity.ok(ApiResponse.success("Configuration applied"));
    }

    @Operation(summary = "Generate a detailed report of detected PIIs")
    @GetMapping("/connections/{connectionId}/report")
    public ResponseEntity<String> generateReport(
            @PathVariable String connectionId,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "0.7") double confidenceThreshold) {

        log.info("Generating PII report for connection: {}, threshold: {}", connectionId, confidenceThreshold);

        piiDetector.setConfidenceThreshold(confidenceThreshold);
        PIIDetectionResult result = piiDetector.detectPII(connectionId, dbType);
        String report = reportGenerator.generateReport(result);

        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_PLAIN)
                .body(report);
    }

    @Operation(summary = "Export PII detection results")
    @GetMapping("/connections/{connectionId}/export")
    public ResponseEntity<byte[]> exportResults(
            @PathVariable String connectionId,
            @RequestParam(required = false) String dbType,
            @RequestParam(defaultValue = "0.7") double confidenceThreshold,
            @RequestParam(defaultValue = "json") String format) {

        log.info("Exporting PII results for connection: {}, format: {}, threshold: {}",
                connectionId, format, confidenceThreshold);

        piiDetector.setConfidenceThreshold(confidenceThreshold);
        PIIDetectionResult result = piiDetector.detectPII(connectionId, dbType);
        byte[] exportData = reportGenerator.exportResults(result, format);

        HttpHeaders headers = new HttpHeaders();

        if ("json".equalsIgnoreCase(format)) {
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setContentDispositionFormData("attachment", "pii_results.json");
        } else if ("csv".equalsIgnoreCase(format)) {
            headers.setContentType(MediaType.parseMediaType("text/csv"));
            headers.setContentDispositionFormData("attachment", "pii_results.csv");
        }

        return new ResponseEntity<>(exportData, headers, HttpStatus.OK);
    }

    // Error handler
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<String>> handleException(Exception e) {
        log.error("Error while processing PII request: {}", e.getMessage(), e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("An error occurred: " + e.getMessage()));
    }
}