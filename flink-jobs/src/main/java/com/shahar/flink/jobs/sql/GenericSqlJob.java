package com.shahar.flink.jobs.sql;

import com.shahar.flink.common.base.AbstractFlinkJob;
import com.shahar.flink.common.utils.SqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * WHAT: Generic SQL job runner that executes SQL files from resources
 * WHY: Enables SQL-as-Code pattern - business logic in SQL files, not Java code
 * HOW: Creates Table API environment, loads SQL files from configuration,
 * executes them in order
 *
 * EXAMPLE USAGE:
 * java -jar flink-jobs.jar \
 * --sql.files
 * "sql/balance/01_source.sql,sql/balance/02_sink.sql,sql/balance/03_logic.sql"
 * \
 * --kafka.bootstrap.servers "kafka:9092" \
 * --schema.registry.url "http://schema-registry:8081"
 */
public class GenericSqlJob extends AbstractFlinkJob<GenericSqlJobConfig> {

    public GenericSqlJob(GenericSqlJobConfig config) {
        super(config);
    }

    @Override
    protected void buildPipeline(StreamExecutionEnvironment env) {
        // Create Table API environment from streaming environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Get SQL files from configuration
        String sqlFilesStr = config.getSqlFiles();
        if (sqlFilesStr == null || sqlFilesStr.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "No SQL files specified. Please set 'sql.files' property with comma-separated file paths.");
        }

        // Split and execute each SQL file in order
        String[] sqlFiles = sqlFilesStr.split(",");
        log.info("Executing {} SQL files in order", sqlFiles.length);

        for (String sqlFile : sqlFiles) {
            String trimmedFile = sqlFile.trim();
            if (!trimmedFile.isEmpty()) {
                try {
                    log.info("Processing SQL file: {}", trimmedFile);
                    SqlUtils.executeSqlFile(tableEnv, trimmedFile, config.getAllProperties());
                } catch (Exception e) {
                    log.error("Failed to execute SQL file: {}", trimmedFile, e);
                    throw new RuntimeException("SQL execution failed for file: " + trimmedFile, e);
                }
            }
        }

        log.info("Successfully configured all SQL statements");
    }

    @Override
    protected String getJobName() {
        return config.getJobName();
    }

    public static void main(String[] args) throws Exception {
        GenericSqlJobConfig config = GenericSqlJobConfig.getInstance();
        GenericSqlJob job = new GenericSqlJob(config);
        job.execute();
    }
}
