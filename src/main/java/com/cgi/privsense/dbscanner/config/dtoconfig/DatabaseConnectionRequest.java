package com.cgi.privsense.dbscanner.config.dtoconfig;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Data transfer object for database connection requests.
 */
@Data
@Builder
public class DatabaseConnectionRequest {
    /**
     * Basic information
     */
    private String name;                // Unique connection name
    private String dbType="mysql";              // Database type (mysql, postgresql, etc.)

    /**
     * Connection information
     */
    private String host;                // Host (localhost, IP, etc.)
    private Integer port;               // Port
    private String database;            // Database name
    private String username;            // Username
    private String password;            // Password

    /**
     * Advanced parameters
     */
    private String driverClassName;     // JDBC driver class name (optional)
    private String url;                 // Complete JDBC URL (optional, will be built if not provided)
    private Map<String, String> properties;  // Additional connection properties

    /**
     * Connection pool parameters
     */
    private Integer maxPoolSize;        // Maximum pool size
    private Integer minIdle;            // Minimum number of idle connections
    private Integer connectionTimeout;  // Connection timeout in ms
    private Boolean autoCommit;         // Auto-commit mode

    /**
     * SSL parameters
     */
    private Boolean useSSL;             // Use SSL
    private String sslMode;             // SSL mode (disable, require, verify-ca, verify-full)
    private String trustStorePath;      // Path to truststore
    private String trustStorePassword;  // Truststore password
}