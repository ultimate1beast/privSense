package com.cgi.privsense.dbscanner.core.scanner;

import java.lang.annotation.*;

/**
 * Annotation to mark a DatabaseScanner implementation with its supported database type.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DatabaseType {
    /**
     * The database type value (mysql, postgresql, etc.)
     */
    String value();
}