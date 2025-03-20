package com.cgi.privsense.piidetector.api;

import java.util.List;

/**
 * Factory for creating PII detection strategies.
 * Implements the Factory pattern.
 */
public interface PIIDetectionStrategyFactory {
    /**
     * Creates a detection strategy by its name.
     *
     * @param strategyName Strategy name
     * @return Strategy instance
     */
    PIIDetectionStrategy createStrategy(String strategyName);

    /**
     * Gets all available strategies.
     *
     * @return List of available strategies
     */
    List<PIIDetectionStrategy> getAllStrategies();

    /**
     * Gets a composite strategy that combines multiple strategies.
     *
     * @param strategyNames Names of strategies to combine
     * @return Composite strategy
     */
    PIIDetectionStrategy createCompositeStrategy(List<String> strategyNames);
}