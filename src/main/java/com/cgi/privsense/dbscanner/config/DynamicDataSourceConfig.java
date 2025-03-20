package com.cgi.privsense.dbscanner.config;

import com.cgi.privsense.dbscanner.config.dtoconfig.DatabaseConnectionRequest;

import com.cgi.privsense.dbscanner.core.datasource.DataSourceProvider;
import com.cgi.privsense.dbscanner.core.driver.DriverManager;
import com.cgi.privsense.dbscanner.exception.DataSourceException;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration for dynamic data sources.
 * Allows creating and switching between different database connections at runtime.
 */
@Slf4j
@Configuration
public class DynamicDataSourceConfig implements DataSourceProvider {

    /**
     * Map of registered data sources.
     */
    private final Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * The routing data source that switches between data sources.
     */
    private final RoutingDataSource routingDataSource;

    /**
     * The driver dependency manager.
     */
    private final DriverManager driverManager;

    /**
     * Map of data source types (mysql, postgresql, etc.).
     */
    private final Map<String, String> dataSourceTypes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param driverManager The driver dependency manager
     */
    public DynamicDataSourceConfig(DriverManager driverManager) {
        this.driverManager = driverManager;
        this.routingDataSource = new RoutingDataSource();
        this.routingDataSource.setTargetDataSources(new HashMap<>());
        this.routingDataSource.setDefaultTargetDataSource(new EmptyDataSource());
        this.routingDataSource.afterPropertiesSet();
    }

    /**
     * Creates a new data source from a connection request.
     *
     * @param request The connection request
     * @return The created data source
     */
    @Override
    public DataSource createDataSource(DatabaseConnectionRequest request) {
        try {
            log.info("Creating data source: {}", request.getName());

            // Ensure driver is available
            String driverClassName = request.getDriverClassName();

            if (driverClassName == null) {
                // Handle the case where dbType might also be null
                String dbType = request.getDbType() != null ? request.getDbType() : "mysql";
                driverClassName = getDriverClassName(dbType);
            }
            driverManager.ensureDriverAvailable(driverClassName);

            DataSourceBuilder<?> builder = DataSourceBuilder.create();

            // Build URL if not provided
            String url = request.getUrl();
            if (url == null) {
                url = buildJdbcUrl(request);
            }

            // Basic configuration
            builder.url(url)
                    .username(request.getUsername())
                    .password(request.getPassword())
                    .driverClassName(driverClassName);

            // Create Hikari data source
            HikariDataSource dataSource = (HikariDataSource) builder.type(HikariDataSource.class).build();

            // Pool configuration
            if (request.getMaxPoolSize() != null) {
                dataSource.setMaximumPoolSize(request.getMaxPoolSize());
            }
            if (request.getMinIdle() != null) {
                dataSource.setMinimumIdle(request.getMinIdle());
            }
            if (request.getConnectionTimeout() != null) {
                dataSource.setConnectionTimeout(request.getConnectionTimeout());
            }
            if (request.getAutoCommit() != null) {
                dataSource.setAutoCommit(request.getAutoCommit());
            }

            // Additional properties
            if (request.getProperties() != null) {
                dataSource.setDataSourceProperties(new Properties() {{
                    putAll(request.getProperties());
                }});
            }

            // Store database type for future reference
            dataSourceTypes.put(request.getName(), request.getDbType() != null ? request.getDbType() : "mysql");

            log.info("Data source created successfully: {}", request.getName());
            return dataSource;
        } catch (Exception e) {
            log.error("Failed to create data source: {}", request.getName(), e);
            throw new DataSourceException("Failed to create data source: " + request.getName(), e);
        }
    }

    /**
     * Builds a JDBC URL from a connection request.
     *
     * @param request The connection request
     * @return The JDBC URL
     */
    private String buildJdbcUrl(DatabaseConnectionRequest request) {
        String dbType = (request.getDbType() != null) ? request.getDbType().toLowerCase() : "mysql";
        return switch (dbType) {
            case "mysql" -> String.format(
                    "jdbc:mysql://%s:%d/%s",
                    request.getHost(),
                    request.getPort() != null ? request.getPort() : 3306,
                    request.getDatabase()
            );
            case "postgresql" -> String.format(
                    "jdbc:postgresql://%s:%d/%s",
                    request.getHost(),
                    request.getPort() != null ? request.getPort() : 5432,
                    request.getDatabase()
            );
            case "oracle" -> String.format(
                    "jdbc:oracle:thin:@%s:%d/%s",
                    request.getHost(),
                    request.getPort() != null ? request.getPort() : 1521,
                    request.getDatabase()
            );
            default -> throw new IllegalArgumentException("Unsupported database type: " + request.getDbType());
        };
    }

    /**
     * Registers a data source.
     *
     * @param name Name of the data source
     * @param dataSource The data source
     */
    @Override
    public void registerDataSource(String name, DataSource dataSource) {
        log.info("Registering data source: {}", name);
        dataSources.put(name, dataSource);
        updateRoutingDataSource();
    }

    /**
     * Updates the routing data source with the current set of data sources.
     */
    private void updateRoutingDataSource() {
        Map<Object, Object> targetDataSources = new HashMap<>(dataSources);
        routingDataSource.setTargetDataSources(targetDataSources);
        routingDataSource.afterPropertiesSet();
    }

    /**
     * Gets the routing data source bean.
     *
     * @return The routing data source
     */
    @Bean
    @Primary
    public DataSource routingDataSource() {
        return routingDataSource;
    }

    /**
     * Gets the driver class name for a database type.
     *
     * @param dbType Database type
     * @return Driver class name
     */
    private String getDriverClassName(String dbType) {
        String type = (dbType != null) ? dbType.toLowerCase() : "mysql";
        return switch (type) {
            case "mysql" -> "com.mysql.cj.jdbc.Driver";
            case "postgresql" -> "org.postgresql.Driver";
            case "oracle" -> "oracle.jdbc.OracleDriver";
            case "sqlserver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
        };
    }

    /**
     * Gets a data source by name.
     *
     * @param name Data source name
     * @return The data source
     */
    @Override
    public DataSource getDataSource(String name) {
        if (!dataSources.containsKey(name)) {
            throw new DataSourceException("DataSource not found: " + name);
        }

        RoutingDataSource.setCurrentDataSource(name);
        return routingDataSource;
    }

    /**
     * Gets the database type for a connection.
     *
     * @param connectionId Connection ID
     * @return Database type
     */
    @Override
    public String getDatabaseType(String connectionId) {
        if (!dataSources.containsKey(connectionId)) {
            throw new IllegalArgumentException("DataSource not found: " + connectionId);
        }

        // Return stored type if available
        if (dataSourceTypes.containsKey(connectionId)) {
            return dataSourceTypes.get(connectionId);
        }

        // Otherwise, detect the type
        try {
            log.debug("Detecting database type for connection: {}", connectionId);
            DataSource dataSource = getDataSource(connectionId);
            try (var connection = dataSource.getConnection()) {
                String productName = connection.getMetaData().getDatabaseProductName();

                // Convert product name to supported type
                String type = switch (productName.toLowerCase()) {
                    case "mysql" -> "mysql";
                    case "postgresql" -> "postgresql";
                    case "oracle" -> "oracle";
                    case "microsoft sql server" -> "sqlserver";
                    default -> throw new IllegalArgumentException("Unsupported database type: " + productName);
                };

                // Store for future use
                dataSourceTypes.put(connectionId, type);
                log.debug("Detected database type for connection {}: {}", connectionId, type);
                return type;
            }
        } catch (Exception e) {
            log.error("Failed to determine database type for connection: {}", connectionId, e);
            throw new RuntimeException("Failed to determine database type for connection: " + connectionId, e);
        } finally {
            RoutingDataSource.clearCurrentDataSource();
        }
    }


    /**
     * Checks if a data source with the given name exists.
     *
     * @param name Data source name
     * @return true if the data source exists, false otherwise
     */
    @Override
    public boolean hasDataSource(String name) {
        return dataSources.containsKey(name);
    }

    /**
     * Removes a registered data source.
     *
     * @param name Name of the data source to remove
     * @return true if the data source was removed successfully, false otherwise
     */
    @Override
    public boolean removeDataSource(String name) {
        log.info("Removing data source: {}", name);
        try {
            // Get the data source before removing it
            DataSource dataSource = dataSources.get(name);

            // If it doesn't exist, we're done
            if (dataSource == null) {
                log.warn("Data source not found: {}", name);
                return false;
            }

            // Remove from the data sources map
            dataSources.remove(name);

            // Also remove from the database type map
            dataSourceTypes.remove(name);

            // Update the routing data source
            updateRoutingDataSource();

            // Close the data source if it's a Hikari data source
            if (dataSource instanceof HikariDataSource) {
                log.info("Closing Hikari connection pool for data source: {}", name);
                ((HikariDataSource) dataSource).close();
            }

            log.info("Data source removed successfully: {}", name);
            return true;
        } catch (Exception e) {
            log.error("Failed to remove data source: {}", name, e);
            return false;
        }
    }

    /**
     * Gets information about all registered data sources.
     *
     * @return List of data source information
     */
    @Override
    public List<Map<String, Object>> getDataSourcesInfo() {
        log.info("Getting information about all data sources");
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map.Entry<String, DataSource> entry : dataSources.entrySet()) {
            String name = entry.getKey();
            DataSource dataSource = entry.getValue();

            Map<String, Object> info = new HashMap<>();
            info.put("name", name);

            // Add database type if available
            if (dataSourceTypes.containsKey(name)) {
                info.put("dbType", dataSourceTypes.get(name));
            }

            // Add additional information for HikariDataSource
            if (dataSource instanceof HikariDataSource) {
                HikariDataSource hikari = (HikariDataSource) dataSource;
                info.put("jdbcUrl", hikari.getJdbcUrl());
                info.put("username", hikari.getUsername());
                info.put("maxPoolSize", hikari.getMaximumPoolSize());
                info.put("minIdle", hikari.getMinimumIdle());
                info.put("connectionTimeout", hikari.getConnectionTimeout());
                info.put("autoCommit", hikari.isAutoCommit());

                // Add connection pool metrics
                try {
                    var poolMXBean = hikari.getHikariPoolMXBean();
                    if (poolMXBean != null) {
                        info.put("activeConnections", poolMXBean.getActiveConnections());
                        info.put("idleConnections", poolMXBean.getIdleConnections());
                        info.put("totalConnections", poolMXBean.getTotalConnections());
                        info.put("threadsAwaitingConnection", poolMXBean.getThreadsAwaitingConnection());
                    } else {
                        log.warn("HikariPoolMXBean is null for data source: {}", name);
                        info.put("poolStatus", "initializing");
                    }
                } catch (Exception e) {
                    log.warn("Failed to get pool metrics for data source: {}", name, e);
                }
            }

            result.add(info);
        }

        return result;
    }
}