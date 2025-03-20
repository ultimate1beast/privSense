package com.cgi.privsense.dbscanner.core.driver;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for database drivers.
 * Maps to properties with the prefix "dbscanner.drivers" in the application properties.
 */
@Component
@ConfigurationProperties(prefix = "dbscanner.drivers")
@Getter
@Setter
public class DriverConfigurationProperties {
    /**
     * Map of Maven coordinates for each driver.
     * Format: "com.mysql.cj.jdbc.Driver" -> "mysql:mysql-connector-java:8.0.33"
     */
    private Map<String, String> coordinates = new HashMap<>();

    /**
     * Directory where downloaded drivers are stored.
     */
    private String directory;

    /**
     * URL of the Maven repository.
     */
    private String repositoryUrl;
}