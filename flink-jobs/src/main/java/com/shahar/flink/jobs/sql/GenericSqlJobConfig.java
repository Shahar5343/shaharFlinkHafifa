package com.shahar.flink.jobs.sql;

import com.shahar.flink.common.base.BaseConfig;

/**
 * WHAT: Configuration for GenericSqlJob
 * WHY: Externalizes SQL file paths and job parameters for environment-specific
 * configuration
 * HOW: Extends BaseConfig and provides getters for SQL-specific properties
 */
public class GenericSqlJobConfig extends BaseConfig {

    private static final String DEFAULT_PROPERTIES_FILE = "application.properties";
    private static GenericSqlJobConfig instance;

    private GenericSqlJobConfig(String propertiesFile) {
        super(propertiesFile);
    }

    /**
     * Singleton instance for default properties file
     */
    public static synchronized GenericSqlJobConfig getInstance() {
        if (instance == null) {
            instance = new GenericSqlJobConfig(DEFAULT_PROPERTIES_FILE);
        }
        return instance;
    }

    /**
     * Get comma-separated list of SQL files to execute in order
     *
     * @return SQL file paths (e.g.,
     *         "sql/balance/01_source.sql,sql/balance/02_sink.sql")
     */
    public String getSqlFiles() {
        return getProperty("sql.files", "");
    }

    /**
     * Get configurable job name
     *
     * @return Job name from properties or default
     */
    public String getJobName() {
        return getProperty("sql.job.name", "Generic SQL Job");
    }
}
