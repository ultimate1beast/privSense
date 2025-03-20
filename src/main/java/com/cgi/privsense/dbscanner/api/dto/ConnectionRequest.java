package com.cgi.privsense.dbscanner.api.dto;

import com.cgi.privsense.dbscanner.config.dtoconfig.DatabaseConnectionRequest;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

/**
 * DTO for connection requests from the API.
 */
@Data
public class ConnectionRequest {
    /**
     * Connection name - will be automatically generated if not provided.
     */
    private String name;

    /**
     * Database type.
     */
    private String dbType;

    /**
     * Database host.
     */
    private String host;

    /**
     * Database port.
     */
    private Integer port;

    /**
     * Database name.
     */
    private String database;

    /**
     * Username.
     */
    private String username;

    /**
     * Password.
     */
    private String password;

    /**
     * JDBC driver class name.
     */
    private String driverClassName;

    /**
     * JDBC URL.
     */
    private String url;

    /**
     * Additional properties.
     */
    private Map<String, String> properties;

    /**
     * Maximum connection pool size.
     */
    private Integer maxPoolSize;

    /**
     * Minimum idle connections.
     */
    private Integer minIdle;

    /**
     * Connection timeout.
     */
    private Integer connectionTimeout;

    /**
     * Auto-commit mode.
     */
    private Boolean autoCommit;

    /**
     * Use SSL.
     */
    private Boolean useSSL;

    /**
     * SSL mode.
     */
    private String sslMode;

    /**
     * Generates a connection identifier if none is provided.
     * Uses a format combining database type, host, port, and database name if available,
     * otherwise falls back to a UUID.
     * @return The generated connection name
     */
    public String generateConnectionId() {
        if (name != null && !name.trim().isEmpty()) {
            return name;
        }

        StringBuilder builder = new StringBuilder();

        // Try to build a meaningful name first
        if (dbType != null) {
            builder.append(dbType.toLowerCase());
        } else if (driverClassName != null) {
            // Extract db type from driver if possible
            if (driverClassName.contains("mysql")) {
                builder.append("mysql");
            } else if (driverClassName.contains("postgresql")) {
                builder.append("postgresql");
            } else if (driverClassName.contains("oracle")) {
                builder.append("oracle");
            } else if (driverClassName.contains("sqlserver")) {
                builder.append("sqlserver");
            } else {
                builder.append("db");
            }
        } else {
            builder.append("db");
        }

        builder.append("_");

        // Add host if available
        if (host != null && !host.isEmpty()) {
            // Replace dots with underscores for hostname
            String sanitizedHost = host.replace('.', '_');
            builder.append(sanitizedHost);

            // Add port if available and not default
            if (port != null) {
                // Check if port is non-default
                boolean isDefaultPort = false;
                if (dbType != null) {
                    switch (dbType.toLowerCase()) {
                        case "mysql":
                            isDefaultPort = port == 3306;
                            break;
                        case "postgresql":
                            isDefaultPort = port == 5432;
                            break;
                        case "oracle":
                            isDefaultPort = port == 1521;
                            break;
                        case "sqlserver":
                            isDefaultPort = port == 1433;
                            break;
                    }
                }

                if (!isDefaultPort) {
                    builder.append("_").append(port);
                }
            }

            // Add database name if available
            if (database != null && !database.isEmpty()) {
                builder.append("_").append(database);
            }
        } else if (url != null) {
            // Try to extract meaningful info from URL
            // Just use a simplified version to avoid complexity
            String urlHash = Integer.toHexString(url.hashCode());
            builder.append("url_").append(urlHash);
        } else {
            // Fallback to UUID if not enough information
            String uniqueId = UUID.randomUUID().toString().substring(0, 8);
            builder.append("conn_").append(uniqueId);
        }

        return builder.toString();
    }

    /**
     * Converts to DatabaseConnectionRequest.
     * Ensures connection name is automatically generated if not provided.
     *
     * @return DatabaseConnectionRequest
     */
    public DatabaseConnectionRequest toConnectionRequest() {
        // Ensure the name is set
        if (name == null || name.trim().isEmpty()) {
            name = generateConnectionId();
        }

        return DatabaseConnectionRequest.builder()
                .name(name)
                .dbType(dbType)
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                .driverClassName(driverClassName)
                .url(url)
                .properties(properties)
                .maxPoolSize(maxPoolSize)
                .minIdle(minIdle)
                .connectionTimeout(connectionTimeout)
                .autoCommit(autoCommit)
                .useSSL(useSSL)
                .sslMode(sslMode)
                .build();
    }
}