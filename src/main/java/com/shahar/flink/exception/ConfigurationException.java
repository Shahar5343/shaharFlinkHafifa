package com.shahar.flink.exception;

/**
 * Exception thrown when configuration is invalid or missing.
 * 
 * BENEFITS:
 * - Fail-fast principle: Catch configuration errors at startup, not runtime
 * - Clear distinction between config errors and runtime errors
 * - Enables specific handling of configuration issues
 * - Better error messages for DevOps/deployment teams
 * 
 * USAGE:
 * Thrown during FlinkConfig initialization or validation when:
 * - Required properties are missing
 * - Property values are invalid (negative numbers, malformed URLs, etc.)
 * - Property combinations are incompatible
 */
public class ConfigurationException extends FlinkJobException {

    private static final long serialVersionUID = 1L;

    private final String propertyKey;

    public ConfigurationException(String message) {
        super(message);
        this.propertyKey = null;
    }

    public ConfigurationException(String propertyKey, String message) {
        super(String.format("Configuration error for property '%s': %s", propertyKey, message));
        this.propertyKey = propertyKey;
    }

    public ConfigurationException(String propertyKey, String message, Throwable cause) {
        super(String.format("Configuration error for property '%s': %s", propertyKey, message), cause);
        this.propertyKey = propertyKey;
    }

    /**
     * Get the property key that caused the error, if available.
     * 
     * @return Property key, or null if error is not specific to a single property
     */
    public String getPropertyKey() {
        return propertyKey;
    }
}
