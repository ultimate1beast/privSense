/*
 * RegexPatternStrategy.java - Optimized strategy using regular expressions to detect PII
 */
package com.cgi.privsense.piidetector.strategy;

import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.enums.DetectionMethod;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Strategy based on regular expressions for PII detection.
 * Uses regex patterns to analyze the content of data samples.
 */
@Component
public class RegexPatternStrategy extends AbstractPIIDetectionStrategy {
    private static final Map<PIIType, Pattern> REGEX_PATTERNS = new HashMap<>();

    // Pre-filters for quick rejection before applying expensive regex
    private static final Map<PIIType, QuickCheckFunction> QUICK_CHECKS = new HashMap<>();

    // Result cache to avoid redundant processing
    private final Map<String, ColumnPIIInfo> resultCache = new ConcurrentHashMap<>();

    static {
        // Email
        REGEX_PATTERNS.put(PIIType.EMAIL,
                Pattern.compile("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"));

        // Phone number (international formats)
        REGEX_PATTERNS.put(PIIType.PHONE_NUMBER,
                Pattern.compile("^(\\+\\d{1,3}[- ]?)?(\\d{3}[- ]?){1,2}\\d{4}$"));

        // Credit card number
        REGEX_PATTERNS.put(PIIType.CREDIT_CARD,
                Pattern.compile("^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\\d{3})\\d{11})$"));

        // IP address
        REGEX_PATTERNS.put(PIIType.IP_ADDRESS,
                Pattern.compile("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"));

        // Postal code (US and FR formats)
        REGEX_PATTERNS.put(PIIType.POSTAL_CODE,
                Pattern.compile("^\\d{5}(?:[-\\s]\\d{4})?$"));

        // Social security number (US format)
        REGEX_PATTERNS.put(PIIType.NATIONAL_ID,
                Pattern.compile("^\\d{3}-\\d{2}-\\d{4}$"));

        // Date in ISO format (YYYY-MM-DD)
        REGEX_PATTERNS.put(PIIType.DATE_OF_BIRTH,
                Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$"));

        // Date and time in ISO format
        REGEX_PATTERNS.put(PIIType.DATE_TIME,
                Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"));

        // Setup quick checks for performance optimization
        QUICK_CHECKS.put(PIIType.CREDIT_CARD, s -> {
            // Credit card numbers should be 13-19 digits without other characters
            return s.length() >= 13 && s.length() <= 19 && s.matches("\\d+");
        });

        QUICK_CHECKS.put(PIIType.EMAIL, s -> {
            // Emails must contain @ character
            return s.contains("@");
        });

        QUICK_CHECKS.put(PIIType.PHONE_NUMBER, s -> {
            // Phone numbers should contain digits and common separators
            return s.matches(".*\\d.*") && s.length() >= 7;
        });

        QUICK_CHECKS.put(PIIType.IP_ADDRESS, s -> {
            // IP addresses must have digits and dots
            return s.matches(".*\\d.*") && s.contains(".");
        });
    }

    @Override
    public String getName() {
        return "RegexPatternStrategy";
    }

    @Override
    public ColumnPIIInfo detectColumnPII(String connectionId, String dbType, String tableName,
                                         String columnName, List<Object> sampleData) {
        // Generate cache key from inputs
        String cacheKey = dbType + ":" + tableName + ":" + columnName + ":" +
                (sampleData != null ? sampleData.hashCode() : "null");

        // Check cache first
        if (resultCache.containsKey(cacheKey)) {
            return resultCache.get(cacheKey);
        }

        ColumnPIIInfo result = ColumnPIIInfo.builder()
                .columnName(columnName)
                .tableName(tableName)
                .piiDetected(false)
                .detections(new ArrayList<>())
                .build();

        if (sampleData == null || sampleData.isEmpty()) {
            return result;
        }

        // Pre-check data type suitability
        boolean isAllNumeric = isAllNumeric(sampleData);
        boolean isAllString = isAllString(sampleData);

        // For each PII type with a regex pattern
        for (Map.Entry<PIIType, Pattern> entry : REGEX_PATTERNS.entrySet()) {
            PIIType piiType = entry.getKey();
            Pattern pattern = entry.getValue();

            // Skip numeric-only checks for string-only data and vice versa
            if ((piiType == PIIType.CREDIT_CARD || piiType == PIIType.NATIONAL_ID) && !isAllNumeric) {
                continue;
            }

            if ((piiType == PIIType.EMAIL || piiType == PIIType.IP_ADDRESS) && !isAllString) {
                continue;
            }

            // Measure match time
            long startTime = System.currentTimeMillis();

            // List of samples that match the pattern
            List<String> matchingData = new ArrayList<>();
            for (Object obj : sampleData) {
                if (obj != null) {
                    String s = obj.toString().trim();

                    // Skip empty strings
                    if (s.isEmpty()) {
                        continue;
                    }

                    // Apply quick check if available
                    if (QUICK_CHECKS.containsKey(piiType) && !QUICK_CHECKS.get(piiType).check(s)) {
                        continue;
                    }

                    // Apply full regex pattern
                    if (pattern.matcher(s).matches()) {
                        matchingData.add(s);
                    }
                }
            }

            // Count matches in the sample
            long matchCount = matchingData.size();

            long matchTime = System.currentTimeMillis() - startTime;

            // Calculate confidence based on match percentage
            double matchRatio = (double) matchCount / sampleData.size();

            // If enough data matches, consider as PII
            if (matchRatio >= confidenceThreshold) {
                // Create detection
                var detection = createDetection(
                        piiType,
                        matchRatio,
                        DetectionMethod.REGEX_PATTERN.name());

                // Add detailed metadata about the detection
                detection.getDetectionMetadata().put("matchRatio", matchRatio);
                detection.getDetectionMetadata().put("sampleSize", sampleData.size());
                detection.getDetectionMetadata().put("matchCount", matchCount);
                detection.getDetectionMetadata().put("matchTimeMs", matchTime);

                // Add some matching examples (limit to 3)
                List<String> matchingExamples = matchingData.stream()
                        .limit(3)
                        .collect(Collectors.toList());
                detection.getDetectionMetadata().put("matchingExamples", matchingExamples);

                // Add the pattern used as string for reference
                detection.getDetectionMetadata().put("patternUsed", pattern.pattern());

                // Add the detection to the result
                result.addDetection(detection);
            }
        }

        // Store in cache
        resultCache.put(cacheKey, result);

        return result;
    }

    @Override
    public boolean isApplicable(boolean hasMetadata, boolean hasSampleData) {
        // This strategy requires data samples
        return hasSampleData;
    }

    /**
     * Checks if all samples are numeric.
     *
     * @param samples List of data samples
     * @return true if all non-null samples are numeric
     */
    private boolean isAllNumeric(List<Object> samples) {
        if (samples == null || samples.isEmpty()) {
            return false;
        }

        for (Object obj : samples) {
            if (obj != null) {
                String s = obj.toString().trim();
                if (!s.isEmpty() && !s.matches("-?\\d+(\\.\\d+)?")) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Checks if all samples are strings.
     *
     * @param samples List of data samples
     * @return true if all non-null samples are strings
     */
    private boolean isAllString(List<Object> samples) {
        if (samples == null || samples.isEmpty()) {
            return false;
        }

        for (Object obj : samples) {
            if (obj != null && !(obj instanceof String)) {
                // Allow objects that can be converted to meaningful strings
                if (!(obj instanceof Number || obj instanceof Date || obj instanceof Boolean)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Clears the result cache.
     */
    public void clearCache() {
        resultCache.clear();
    }

    /**
     * Functional interface for quick pattern pre-checks.
     */
    @FunctionalInterface
    private interface QuickCheckFunction {
        boolean check(String input);
    }
}