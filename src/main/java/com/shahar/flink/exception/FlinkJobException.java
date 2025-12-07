package com.shahar.flink.exception;

/**
 * Base exception for all Flink job-related errors.
 * 
 * BENEFITS:
 * - Distinguishes application errors from system/library errors
 * - Enables centralized error handling and logging
 * - Allows catching all job-specific exceptions in one catch block
 * - Supports error categorization for monitoring
 */
public class FlinkJobException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public FlinkJobException(String message) {
        super(message);
    }

    public FlinkJobException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlinkJobException(Throwable cause) {
        super(cause);
    }
}
