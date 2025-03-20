/*
 * PIIDetectionStrategyFactoryImpl.java - Factory implementation for detection strategies
 */
package com.cgi.privsense.piidetector.service;

import com.cgi.privsense.piidetector.api.PIIDetectionStrategy;
import com.cgi.privsense.piidetector.api.PIIDetectionStrategyFactory;
import com.cgi.privsense.piidetector.strategy.CompositePIIDetectionStrategy;
import com.cgi.privsense.piidetector.strategy.HeuristicNameStrategy;
import com.cgi.privsense.piidetector.strategy.NERModelStrategy;
import com.cgi.privsense.piidetector.strategy.RegexPatternStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating PII detection strategies.
 * Implements the Factory pattern.
 */
@Component
public class PIIDetectionStrategyFactoryImpl implements PIIDetectionStrategyFactory {
    private static final Logger log = LoggerFactory.getLogger(PIIDetectionStrategyFactoryImpl.class);

    private final Map<String, PIIDetectionStrategy> strategies = new ConcurrentHashMap<>();

    // Cache for composite strategies to avoid recreation
    private final Map<String, PIIDetectionStrategy> compositeStrategiesCache = new ConcurrentHashMap<>();

    @Autowired
    public PIIDetectionStrategyFactoryImpl(
            HeuristicNameStrategy heuristicStrategy,
            RegexPatternStrategy regexStrategy,
            NERModelStrategy nerStrategy,
            CompositePIIDetectionStrategy compositeStrategy) {

        // Register available strategies
        strategies.put(heuristicStrategy.getName(), heuristicStrategy);
        strategies.put(regexStrategy.getName(), regexStrategy);
        strategies.put(nerStrategy.getName(), nerStrategy);
        strategies.put(compositeStrategy.getName(), compositeStrategy);

        // The composite strategy is no longer automatically configured with all strategies
        // in this pipeline approach, but kept for compatibility with existing code

        log.info("PII detection strategy factory initialized with {} strategies", strategies.size());
    }

    @Override
    public PIIDetectionStrategy createStrategy(String strategyName) {
        PIIDetectionStrategy strategy = strategies.get(strategyName);
        if (strategy == null) {
            throw new IllegalArgumentException("Unknown strategy: " + strategyName);
        }
        return strategy;
    }

    @Override
    public List<PIIDetectionStrategy> getAllStrategies() {
        return new ArrayList<>(strategies.values());
    }

    @Override
    public PIIDetectionStrategy createCompositeStrategy(List<String> strategyNames) {
        // Sort strategy names to ensure consistent cache keys
        List<String> sortedNames = new ArrayList<>(strategyNames);
        Collections.sort(sortedNames);

        // Create cache key
        String cacheKey = String.join("_", sortedNames);

        // Check cache first
        if (compositeStrategiesCache.containsKey(cacheKey)) {
            return compositeStrategiesCache.get(cacheKey);
        }

        // Create new composite strategy
        CompositePIIDetectionStrategy composite = new CompositePIIDetectionStrategy();

        for (String name : sortedNames) {
            PIIDetectionStrategy strategy = createStrategy(name);
            composite.addStrategy(strategy);
        }

        // Cache the result
        compositeStrategiesCache.put(cacheKey, composite);

        return composite;
    }

    /**
     * Clears the composite strategies cache.
     */
    public void clearCompositeCache() {
        compositeStrategiesCache.clear();
    }
}