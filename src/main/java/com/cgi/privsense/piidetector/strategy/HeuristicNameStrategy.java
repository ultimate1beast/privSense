/*
 * HeuristicNameStrategy.java - Optimized strategy using column names to detect PII
 */
package com.cgi.privsense.piidetector.strategy;

import com.cgi.privsense.piidetector.model.ColumnPIIInfo;
import com.cgi.privsense.piidetector.model.enums.DetectionMethod;
import com.cgi.privsense.piidetector.model.enums.PIIType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Component
public class HeuristicNameStrategy extends AbstractPIIDetectionStrategy {
    private static final Map<PIIType, List<String>> NAME_PATTERNS = new HashMap<>();
    private static final Map<PIIType, Pattern> REGEX_NAME_PATTERNS = new HashMap<>();

    // Cache for normalized column names
    private final Map<String, String> normalizedNameCache = new ConcurrentHashMap<>();

    // Cache for detection results
    private final Map<String, ColumnPIIInfo> detectionCache = new ConcurrentHashMap<>();

    static {
        // Initialize name patterns for each PII type
        NAME_PATTERNS.put(PIIType.FULL_NAME, Arrays.asList(
                "name", "full_name", "fullname", "complete_name", "person_name", "customer_name", "user_name", "nom_complet"));

        NAME_PATTERNS.put(PIIType.FIRST_NAME, Arrays.asList(
                "first_name", "firstname", "forename", "given_name", "prenom", "fname"));

        NAME_PATTERNS.put(PIIType.LAST_NAME, Arrays.asList(
                "last_name", "lastname", "surname", "family_name", "nom", "lname"));

        NAME_PATTERNS.put(PIIType.EMAIL, Arrays.asList(
                "email", "email_address", "mail", "e_mail", "courriel", "mail_address"));

        NAME_PATTERNS.put(PIIType.PHONE_NUMBER, Arrays.asList(
                "phone", "phone_number", "telephone", "mobile", "cell", "contact_number", "numero_telephone", "tel"));

        NAME_PATTERNS.put(PIIType.ADDRESS, Arrays.asList(
                "address", "street_address", "full_address", "postal_address", "mailing_address", "adresse"));

        NAME_PATTERNS.put(PIIType.CITY, Arrays.asList(
                "city", "town", "locality", "ville", "ciudad"));

        NAME_PATTERNS.put(PIIType.POSTAL_CODE, Arrays.asList(
                "postal_code", "zip", "zip_code", "postcode", "code_postal"));

        NAME_PATTERNS.put(PIIType.NATIONAL_ID, Arrays.asList(
                "ssn", "social_security_number", "national_id", "id_number", "fiscal_code", "tax_id",
                "identity_number", "numero_securite_sociale", "nin", "nino"));

        NAME_PATTERNS.put(PIIType.CREDIT_CARD, Arrays.asList(
                "credit_card", "cc_number", "card_number", "creditcard", "payment_card", "numero_carte"));

        NAME_PATTERNS.put(PIIType.DATE_OF_BIRTH, Arrays.asList(
                "birth_date", "date_of_birth", "birthdate", "dob", "birth_day", "date_naissance"));

        NAME_PATTERNS.put(PIIType.PASSWORD, Arrays.asList(
                "password", "pwd", "pass", "user_password", "user_pass", "secret", "credentials"));

        // More robust regex patterns for column names
        REGEX_NAME_PATTERNS.put(PIIType.EMAIL, Pattern.compile(".*e[-_]?mail.*|.*mail[-_]?address.*", Pattern.CASE_INSENSITIVE));
        REGEX_NAME_PATTERNS.put(PIIType.PHONE_NUMBER, Pattern.compile(".*phone.*|.*mobile.*|.*cell.*|.*tel.*", Pattern.CASE_INSENSITIVE));
        REGEX_NAME_PATTERNS.put(PIIType.CREDIT_CARD, Pattern.compile(".*credit.*card.*|.*card.*number.*|.*cc.*num.*", Pattern.CASE_INSENSITIVE));
        REGEX_NAME_PATTERNS.put(PIIType.DATE_OF_BIRTH, Pattern.compile(".*birth.*date.*|.*dob.*|.*born.*", Pattern.CASE_INSENSITIVE));
    }

    @Override
    public String getName() {
        return "HeuristicNameStrategy";
    }

    @Override
    public ColumnPIIInfo detectColumnPII(String connectionId, String dbType, String tableName,
                                         String columnName, List<Object> sampleData) {
        // Use cache for previously analyzed columns
        String cacheKey = dbType + ":" + tableName + ":" + columnName;
        if (detectionCache.containsKey(cacheKey)) {
            return detectionCache.get(cacheKey);
        }

        ColumnPIIInfo result = ColumnPIIInfo.builder()
                .columnName(columnName)
                .tableName(tableName)
                .piiDetected(false)
                .detections(new ArrayList<>())
                .build();

        // Normalized column name for comparison
        String normalizedName = getNormalizedName(columnName);

        // Check regex patterns first (more accurate)
        for (Map.Entry<PIIType, Pattern> entry : REGEX_NAME_PATTERNS.entrySet()) {
            PIIType piiType = entry.getKey();
            Pattern pattern = entry.getValue();

            if (pattern.matcher(normalizedName).matches()) {
                // Higher confidence for regex pattern matches
                result.addDetection(createDetection(
                        piiType,
                        0.85,
                        DetectionMethod.HEURISTIC_NAME_BASED.name()));

                // Skip exact matches for this type
                continue;
            }
        }

        // Search for matches in name patterns
        for (Map.Entry<PIIType, List<String>> entry : NAME_PATTERNS.entrySet()) {
            PIIType piiType = entry.getKey();
            List<String> patterns = entry.getValue();

            // Skip if we already detected this type via regex
            if (result.getDetections().stream()
                    .anyMatch(d -> d.getPiiType() == piiType)) {
                continue;
            }

            for (String pattern : patterns) {
                // Calculate confidence based on the match
                double confidence = calculateNameMatchConfidence(normalizedName, pattern);

                if (confidence >= confidenceThreshold) {
                    result.addDetection(createDetection(
                            piiType,
                            confidence,
                            DetectionMethod.HEURISTIC_NAME_BASED.name()));
                    break;  // One detection per PII type
                }
            }
        }

        // Consider column context for enhanced detection accuracy
        enhanceContextualMatching(result, tableName, columnName);

        // Cache the result
        detectionCache.put(cacheKey, result);

        return result;
    }

    @Override
    public boolean isApplicable(boolean hasMetadata, boolean hasSampleData) {
        // This strategy only requires metadata (column names)
        return hasMetadata;
    }

    /**
     * Calculates the confidence level of the match between a column name and a pattern.
     *
     * @param columnName Normalized column name
     * @param pattern Pattern to search for
     * @return Confidence level between 0.0 and 1.0
     */
    private double calculateNameMatchConfidence(String columnName, String pattern) {
        // Exact match
        if (columnName.equals(pattern)) {
            return 1.0;
        }

        // Match at the beginning or end with a delimiter
        if (columnName.startsWith(pattern + "_") || columnName.endsWith("_" + pattern)) {
            return 0.9;
        }

        // Contains the pattern with delimiters
        if (columnName.contains("_" + pattern + "_")) {
            return 0.8;
        }

        // Contains the pattern without delimiters
        if (columnName.contains(pattern)) {
            return 0.7;
        }

        return 0.0;
    }

    /**
     * Gets normalized column name from cache or normalizes it.
     *
     * @param columnName Original column name
     * @return Normalized name for comparison
     */
    private String getNormalizedName(String columnName) {
        return normalizedNameCache.computeIfAbsent(columnName,
                name -> name.toLowerCase().replaceAll("[^a-z0-9_]", ""));
    }

    /**
     * Enhances detection based on column context (table name, adjacent columns).
     *
     * @param result Column PII info to enhance
     * @param tableName Table name
     * @param columnName Column name
     */
    private void enhanceContextualMatching(ColumnPIIInfo result, String tableName, String columnName) {
        String normalizedTableName = getNormalizedName(tableName);

        // Table name contains user/customer - increase confidence for personal data
        if (normalizedTableName.contains("user") ||
                normalizedTableName.contains("customer") ||
                normalizedTableName.contains("person") ||
                normalizedTableName.contains("employee")) {

            for (PIIType personalType : Arrays.asList(PIIType.FULL_NAME, PIIType.EMAIL,
                    PIIType.PHONE_NUMBER, PIIType.ADDRESS)) {
                result.getDetections().stream()
                        .filter(d -> d.getPiiType() == personalType)
                        .forEach(d -> {
                            // Enhance confidence, but cap it at 0.95
                            double enhancedConfidence = Math.min(d.getConfidence() + 0.1, 0.95);
                            d.setConfidence(enhancedConfidence);
                            d.getDetectionMetadata().put("contextEnhanced", true);
                        });
            }
        }

        // Handle proximity-based enhancement
        // This would use information about adjacent columns in the same table
        // For now, we'll just use a simple pattern match for common groupings

        String normalizedColName = getNormalizedName(columnName);

        // Enhance first_name confidence if last_name exists and vice versa
        if (normalizedColName.contains("first") && !result.isPiiDetected()) {
            result.addDetection(createDetection(
                    PIIType.FIRST_NAME,
                    0.65,  // Below normal threshold, but might be enhanced by context
                    DetectionMethod.HEURISTIC_NAME_BASED.name()));

            // Add a note about this low-confidence detection
            result.getDetections().get(0).getDetectionMetadata()
                    .put("note", "Low confidence detection based on column name pattern");
        } else if (normalizedColName.contains("last") && !result.isPiiDetected()) {
            result.addDetection(createDetection(
                    PIIType.LAST_NAME,
                    0.65,  // Below normal threshold, but might be enhanced by context
                    DetectionMethod.HEURISTIC_NAME_BASED.name()));

            // Add a note about this low-confidence detection
            result.getDetections().get(0).getDetectionMetadata()
                    .put("note", "Low confidence detection based on column name pattern");
        }
    }

    /**
     * Clears the detection cache.
     * Should be called when confidence threshold changes.
     */
    public void clearCache() {
        detectionCache.clear();
    }
}