package com.cgi.privsense.piidetector.model.enums;

/**
 * Enumeration of PII types.
 */
public enum PIIType {
    // Personal identification information
    FULL_NAME,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE_NUMBER,
    ADDRESS,
    CITY,
    POSTAL_CODE,
    COUNTRY,

    // Sensitive data
    NATIONAL_ID,      // Social security number, national ID card, etc.
    PASSPORT_NUMBER,
    DRIVING_LICENSE,
    CREDIT_CARD,
    BANK_ACCOUNT,

    // Medical and biometric data
    HEALTH_INFO,
    BIOMETRIC_DATA,

    // Location data
    GEOLOCATION,
    IP_ADDRESS,

    // Online identifiers
    USERNAME,
    PASSWORD,

    // Other
    DATE_OF_BIRTH,
    GENDER,
    ETHNICITY,
    RELIGION,
    POLITICAL_OPINION,
    DATE_TIME,
    SALARY, UNKNOWN
}