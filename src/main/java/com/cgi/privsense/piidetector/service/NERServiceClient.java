package com.cgi.privsense.piidetector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client for the external NER (Named Entity Recognition) service.
 * Uses RestTemplate to communicate with the Python service.
 */
@Component
public class NERServiceClient {
    private static final Logger log = LoggerFactory.getLogger(NERServiceClient.class);

    private final String nerServiceUrl;
    private final RestTemplate restTemplate;
    private final boolean isLocalService;

    // Cache for NER results to avoid redundant calls
    private final Map<String, Map<String, Double>> nerResultsCache = new ConcurrentHashMap<>();

    /**
     * Constructor
     *
     * @param nerServiceUrl NER service URL
     * @param trustAllCerts Whether to trust all certificates (default: false)
     */
    public NERServiceClient(
            @Value("${piidetector.ner.service.url}") String nerServiceUrl,
            @Value("${piidetector.ner.trust.all.certs:false}") boolean trustAllCerts) {

        this.nerServiceUrl = nerServiceUrl;
        this.isLocalService = isLocalService(nerServiceUrl);

        // Create an appropriate RestTemplate based on whether this is a local service
        if (isLocalService && !trustAllCerts) {
            // Standard RestTemplate for local services
            SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
            requestFactory.setConnectTimeout(5000);  // 5 seconds
            requestFactory.setReadTimeout(30000);    // 30 seconds
            this.restTemplate = new RestTemplate(requestFactory);
            log.info("Using standard RestTemplate for local service at: {}", nerServiceUrl);
        } else {
            // Trust all RestTemplate for non-local services or if explicitly requested
            this.restTemplate = createStandardRestTemplate();
            log.info("Using standard RestTemplate with normal SSL handling for: {}", nerServiceUrl);
        }

        log.info("NER Service Client initialized with URL: {}", nerServiceUrl);
    }

    /**
     * Determine if the service URL is local
     *
     * @param url The service URL
     * @return true if local, false otherwise
     */
    private boolean isLocalService(String url) {
        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            return host == null || host.equals("localhost") || host.equals("127.0.0.1") || host.startsWith("192.168.") || host.startsWith("10.");
        } catch (URISyntaxException e) {
            log.warn("Invalid URI syntax for service URL: {}", url);
            return false;
        }
    }

    /**
     * Batch analysis of text samples with the NER service.
     * Processes multiple columns in a single API call to reduce network overhead.
     *
     * @param columnDataMap Map of column names to text samples
     * @return Map of column names to detected entity types with confidence levels
     */
    public Map<String, Map<String, Double>> batchAnalyzeText(Map<String, List<String>> columnDataMap) {
        if (columnDataMap == null || columnDataMap.isEmpty()) {
            log.warn("No columns to analyze");
            return Collections.emptyMap();
        }

        Map<String, Map<String, Double>> results = new HashMap<>();
        List<String> allSamples = new ArrayList<>();
        Map<String, Integer> sampleCounts = new HashMap<>();
        Map<String, Integer> sampleStartIndices = new HashMap<>();

        // Prepare all samples for batch processing
        int currentIndex = 0;
        for (Map.Entry<String, List<String>> entry : columnDataMap.entrySet()) {
            String columnName = entry.getKey();
            List<String> samples = entry.getValue();

            if (samples == null || samples.isEmpty()) {
                results.put(columnName, Collections.emptyMap());
                continue;
            }

            // Check cache first
            String cacheKey = columnName + "_" + String.join("_", samples);
            if (nerResultsCache.containsKey(cacheKey)) {
                results.put(columnName, nerResultsCache.get(cacheKey));
                continue;
            }

            sampleStartIndices.put(columnName, currentIndex);
            sampleCounts.put(columnName, samples.size());
            allSamples.addAll(samples);
            currentIndex += samples.size();
        }

        // If all results were from cache or no samples to process
        if (allSamples.isEmpty()) {
            return results;
        }

        try {
            // Prepare the batch request
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("texts", allSamples);

            log.debug("Sending {} text samples to NER service in batch mode", allSamples.size());

            // Send the request to the NER service
            ResponseEntity<Map<String, List<Map<String, Double>>>> response = restTemplate.exchange(
                    nerServiceUrl + "/batch",
                    HttpMethod.POST,
                    new HttpEntity<>(requestBody),
                    new ParameterizedTypeReference<Map<String, List<Map<String, Double>>>>() {});

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, List<Map<String, Double>>> batchResults = response.getBody();

                // Process results for each column
                for (Map.Entry<String, Integer> entry : sampleCounts.entrySet()) {
                    String columnName = entry.getKey();
                    int count = entry.getValue();
                    int startIndex = sampleStartIndices.get(columnName);

                    // Extract and aggregate results for this column
                    Map<String, Double> columnResults = aggregateColumnResults(
                            batchResults.get("results").subList(startIndex, startIndex + count));

                    results.put(columnName, columnResults);

                    // Cache the results
                    String cacheKey = columnName + "_" + String.join("_",
                            columnDataMap.get(columnName));
                    nerResultsCache.put(cacheKey, columnResults);
                }

                return results;
            } else {
                log.error("Error when calling NER service: {}", response.getStatusCode());
                return results;
            }
        } catch (Exception e) {
            log.error("Exception when calling NER service: {}", e.getMessage(), e);
            return results;
        }
    }

    /**
     * Aggregates individual sample results for a column into a single confidence map.
     *
     * @param sampleResults List of entity detection results for each sample
     * @return Map of entity types to confidence levels
     */
    private Map<String, Double> aggregateColumnResults(List<Map<String, Double>> sampleResults) {
        Map<String, List<Double>> entityConfidences = new HashMap<>();

        // Collect all confidences for each entity type
        for (Map<String, Double> result : sampleResults) {
            for (Map.Entry<String, Double> entity : result.entrySet()) {
                entityConfidences
                        .computeIfAbsent(entity.getKey(), k -> new ArrayList<>())
                        .add(entity.getValue());
            }
        }

        // Calculate average confidence for each entity type
        Map<String, Double> aggregatedResults = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : entityConfidences.entrySet()) {
            // Use 75th percentile as confidence to reduce impact of outliers
            List<Double> confidences = entry.getValue();
            Collections.sort(confidences);
            int p75Index = (int)(confidences.size() * 0.75);
            Double confidenceValue = confidences.get(Math.min(p75Index, confidences.size() - 1));

            aggregatedResults.put(entry.getKey(), confidenceValue);
        }

        return aggregatedResults;
    }

    /**
     * Analyzes text with the NER service.
     *
     * @param textSamples List of text samples to analyze
     * @return Map of detected entity types with their confidence level
     */
    public Map<String, Double> analyzeText(List<String> textSamples) {
        if (textSamples == null || textSamples.isEmpty()) {
            log.warn("No text to analyze");
            return Collections.emptyMap();
        }

        // Check cache first
        String cacheKey = String.join("_", textSamples);
        if (nerResultsCache.containsKey(cacheKey)) {
            return nerResultsCache.get(cacheKey);
        }

        try {
            // Prepare the request
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("texts", textSamples);

            log.debug("Sending {} text samples to NER service", textSamples.size());

            // Send the request to the NER service
            ResponseEntity<Map<String, Double>> response = restTemplate.exchange(
                    nerServiceUrl,
                    HttpMethod.POST,
                    new HttpEntity<>(requestBody),
                    new ParameterizedTypeReference<Map<String, Double>>() {});

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                log.debug("NER results received: {}", response.getBody());

                // Cache the results
                nerResultsCache.put(cacheKey, response.getBody());

                return response.getBody();
            } else {
                log.error("Error when calling NER service: {}", response.getStatusCode());
                return Collections.emptyMap();
            }
        } catch (Exception e) {
            log.error("Exception when calling NER service: {}", e.getMessage(), e);
            return Collections.emptyMap();
        }
    }

    /**
     * Creates a standard RestTemplate with reasonable timeouts.
     *
     * @return Configured RestTemplate
     */
    private RestTemplate createStandardRestTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory() {
            @Override
            protected void prepareConnection(HttpURLConnection connection, String httpMethod) throws IOException {
                super.prepareConnection(connection, httpMethod);
            }
        };

        // Set reasonable timeouts
        requestFactory.setConnectTimeout(5000);  // 5 seconds
        requestFactory.setReadTimeout(30000);    // 30 seconds

        return new RestTemplate(requestFactory);
    }

    /**
     * Checks if the NER service is available.
     *
     * @return true if the service is available
     */
    public boolean isServiceAvailable() {
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(
                    nerServiceUrl.replace("/ner", "/health"),
                    String.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            log.warn("NER service not available: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Clears the NER results cache.
     */
    public void clearCache() {
        nerResultsCache.clear();
        log.info("NER results cache cleared");
    }
}