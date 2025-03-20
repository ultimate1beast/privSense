package com.cgi.privsense.dbscanner.core.driver;

import com.cgi.privsense.dbscanner.exception.DriverException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of DriverManager that downloads drivers from Maven Central.
 */
@Slf4j
@Component
public class MavenDriverManager implements DriverManager {
    /**
     * Map of driver class names to Maven coordinates.
     */
    private final Map<String, String> driverMavenCoordinates;

    /**
     * Map of loaded drivers.
     */
    private final Map<String, Driver> loadedDrivers = new ConcurrentHashMap<>();

    /**
     * Directory where downloaded drivers are stored.
     */
    private final Path driversDir;

    /**
     * Constructor.
     *
     * @param driversDirectory Directory for storing downloaded drivers
     * @param repositoryUrl Maven repository URL
     * @param driverConfigProps Driver configuration properties
     */
    public MavenDriverManager(
            @Value("${dbscanner.drivers.directory:${user.home}/.dbscanner/drivers}") String driversDirectory,
            @Value("${dbscanner.drivers.repository-url:https://repo1.maven.org/maven2}") String repositoryUrl,
            DriverConfigurationProperties driverConfigProps
    ) {
        this.driverMavenCoordinates = driverConfigProps.getCoordinates();
        this.driversDir = Paths.get(driversDirectory);

        try {
            // Create the drivers directory if it doesn't exist
            Files.createDirectories(driversDir);
        } catch (Exception e) {
            log.error("Failed to create drivers directory: {}", driversDir, e);
            throw new DriverException("Failed to create drivers directory", e);
        }
    }

    @Override
    public void ensureDriverAvailable(String driverClassName) throws DriverException {
        if (isDriverLoaded(driverClassName)) {
            log.debug("Driver already loaded: {}", driverClassName);
            return;
        }

        try {
            // Get the Maven coordinates for the driver
            String mavenCoordinates = driverMavenCoordinates.get(driverClassName);
            if (mavenCoordinates == null) {
                throw new DriverException("Unknown driver: " + driverClassName);
            }

            // Download and load the driver
            Path driverJar = downloadDriver(mavenCoordinates);
            loadDriver(driverClassName, driverJar);

        } catch (Exception e) {
            log.error("Failed to load driver: {}", driverClassName, e);
            throw new DriverException("Failed to load driver: " + driverClassName, e);
        }
    }

    @Override
    public boolean isDriverLoaded(String driverClassName) {
        // First check if we've loaded it already
        if (loadedDrivers.containsKey(driverClassName)) {
            return true;
        }

        // Otherwise, check if the class is available
        try {
            Class.forName(driverClassName);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public Driver getDriver(String driverClassName) {
        return loadedDrivers.get(driverClassName);
    }

    /**
     * Downloads a driver JAR from Maven Central.
     *
     * @param mavenCoordinates Maven coordinates of the driver
     * @return Path to the downloaded JAR
     */
    private Path downloadDriver(String mavenCoordinates) {
        String[] parts = mavenCoordinates.split(":");
        String groupId = parts[0].replace('.', '/');
        String artifactId = parts[1];
        String version = parts[2];

        Path jarPath = driversDir.resolve(artifactId + "-" + version + ".jar");

        if (Files.exists(jarPath)) {
            log.debug("Driver jar already exists: {}", jarPath);
            return jarPath;
        }

        URI uri = URI.create(String.format(
                "https://repo1.maven.org/maven2/%s/%s/%s/%s-%s.jar",
                groupId, artifactId, version, artifactId, version
        ));

        try {
            log.info("Downloading driver from: {}", uri);
            Files.copy(uri.toURL().openStream(), jarPath);
            log.info("Driver downloaded successfully: {}", jarPath);
            return jarPath;
        } catch (Exception e) {
            throw new DriverException("Failed to download driver: " + uri, e);
        }
    }

    /**
     * Loads a driver from a JAR file.
     *
     * @param driverClassName Name of the driver class
     * @param jarPath Path to the JAR file
     */
    private void loadDriver(String driverClassName, Path jarPath) {
        try {
            // Create a URL class loader with the driver JAR
            URLClassLoader classLoader = new URLClassLoader(
                    new URL[]{jarPath.toUri().toURL()},
                    getClass().getClassLoader()
            );

            // Load and instantiate the driver
            Class<?> driverClass = Class.forName(driverClassName, true, classLoader);
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();

            // Register the driver with a shim to avoid classloader issues
            java.sql.DriverManager.registerDriver(new DriverShim(driver));

            // Store the driver in our cache
            loadedDrivers.put(driverClassName, driver);
            log.info("Successfully loaded driver: {}", driverClassName);

        } catch (Exception e) {
            throw new DriverException("Failed to load driver class: " + driverClassName, e);
        }
    }
}