package com.cgi.privsense.dbscanner.core.scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory for creating database scanners.
 */
@Component
public class DatabaseScannerFactory {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseScannerFactory.class);

    /**
     * Map of database types to scanner classes.
     */
    private final Map<String, Class<? extends DatabaseScanner>> scannerTypes;

    /**
     * Constructor.
     * Finds all scanners with the @DatabaseType annotation.
     *
     * @param applicationContext Spring application context
     */
    public DatabaseScannerFactory(ApplicationContext applicationContext) {
        this.scannerTypes = applicationContext.getBeansOfType(DatabaseScanner.class)
                .values()
                .stream()
                .filter(scanner -> scanner.getClass().isAnnotationPresent(DatabaseType.class))
                .collect(Collectors.toMap(
                        scanner -> scanner.getClass()
                                .getAnnotation(DatabaseType.class)
                                .value()
                                .toLowerCase(),
                        DatabaseScanner::getClass
                ));

        logger.info("Registered database scanners: {}", scannerTypes.keySet());
    }

    /**
     * Gets a scanner for the specified database type.
     *
     * @param dbType Database type
     * @param dataSource Data source
     * @return Database scanner
     */
    public DatabaseScanner getScanner(String dbType, DataSource dataSource) {
        Class<? extends DatabaseScanner> scannerClass = scannerTypes.get(dbType.toLowerCase());
        if (scannerClass == null) {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }

        // Create a new instance of the scanner with the data source
        try {
            logger.debug("Creating scanner for database type: {}", dbType);
            return scannerClass.getConstructor(DataSource.class)
                    .newInstance(dataSource);
        } catch (Exception e) {
            logger.error("Failed to create scanner for type: {}", dbType, e);
            throw new RuntimeException("Failed to create scanner for type: " + dbType, e);
        }
    }

    /**
     * Gets the list of supported database types.
     *
     * @return List of supported database types
     */
    public List<String> getSupportedDatabaseTypes() {
        return List.copyOf(scannerTypes.keySet());
    }
}