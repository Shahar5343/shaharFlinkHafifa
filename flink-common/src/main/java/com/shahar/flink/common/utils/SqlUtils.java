package com.shahar.flink.common.utils;

import org.apache.commons.io.IOUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * WHAT: Utility class for loading and executing SQL files from resources
 * WHY: Enables SQL-as-Code pattern - separates business logic (SQL) from
 * execution framework (Java)
 * HOW: Loads SQL files, substitutes variables from properties, executes via
 * Flink Table API
 */
public class SqlUtils implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SqlUtils.class);

    /**
     * Loads a SQL file from resources, injects parameters, and executes it.
     *
     * @param tableEnv     StreamTableEnvironment for executing SQL
     * @param resourcePath Path to SQL file in resources (e.g.,
     *                     "sql/balance_aggregation/01_source.sql")
     * @param properties   Properties containing parameter values for substitution
     * @throws IOException if SQL file cannot be read
     */
    public static void executeSqlFile(
            StreamTableEnvironment tableEnv,
            String resourcePath,
            Properties properties) throws IOException {

        // 1. Load SQL file from resources
        String sql = loadResource(resourcePath);

        // 2. Substitute variables (e.g., ${kafka.bootstrap.servers})
        String processedSql = substituteParameters(sql, properties);

        // 3. Execute SQL statement
        LOG.info("Executing SQL from: {}", resourcePath);
        LOG.debug("SQL content:\n{}", processedSql);

        try {
            tableEnv.executeSql(processedSql);
            LOG.info("Successfully executed SQL from: {}", resourcePath);
        } catch (Exception e) {
            LOG.error("Failed to execute SQL from: {}", resourcePath, e);
            throw new RuntimeException("SQL execution failed for: " + resourcePath, e);
        }
    }

    /**
     * Loads a resource file from classpath.
     *
     * @param path Path to resource file
     * @return Content of the resource file as a String
     * @throws IOException if resource cannot be found or read
     */
    private static String loadResource(String path) throws IOException {
        try (InputStream is = SqlUtils.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IOException("SQL file not found in classpath: " + path);
            }
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        }
    }

    /**
     * Substitutes ${key} placeholders in SQL with values from properties.
     *
     * @param sql        SQL string with placeholders (e.g.,
     *                   "${kafka.bootstrap.servers}")
     * @param properties Properties containing values for substitution
     * @return SQL string with placeholders replaced by actual values
     */
    private static String substituteParameters(String sql, Properties properties) {
        String result = sql;

        // Iterate through all properties and replace placeholders
        for (String key : properties.stringPropertyNames()) {
            String placeholder = "${" + key + "}";
            String value = properties.getProperty(key);

            if (result.contains(placeholder)) {
                LOG.debug("Substituting {} -> {}", placeholder, value);
                result = result.replace(placeholder, value);
            }
        }

        // Check for unresolved placeholders (helps catch configuration errors)
        if (result.contains("${")) {
            LOG.warn("SQL contains unresolved placeholders. Please check your configuration.");
            // Extract and log unresolved placeholders for debugging
            int startIdx = result.indexOf("${");
            while (startIdx != -1) {
                int endIdx = result.indexOf("}", startIdx);
                if (endIdx != -1) {
                    String unresolvedKey = result.substring(startIdx + 2, endIdx);
                    LOG.warn("Unresolved placeholder: ${{{}}}", unresolvedKey);
                }
                startIdx = result.indexOf("${", startIdx + 1);
            }
        }

        return result;
    }
}
