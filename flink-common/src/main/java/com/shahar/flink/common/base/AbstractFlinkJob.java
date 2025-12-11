package com.shahar.flink.common.base;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFlinkJob<C extends BaseConfig> {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final C config;
    protected final StreamExecutionEnvironment env;

    protected AbstractFlinkJob(C config) {
        this.config = config;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public void execute() throws Exception {
        log.info("Starting {}", getJobName());
        log.info("Configuration: {}", config.getAllProperties());

        setupEnvironment();
        buildPipeline(env);

        log.info("Executing {}", getJobName());
        env.execute(getJobName());
    }

    protected void setupEnvironment() {
        configureCheckpointing();
    }

    protected void configureCheckpointing() {
        env.enableCheckpointing(config.getCheckpointInterval());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        checkpointConfig.setCheckpointTimeout(600000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
    }

    protected abstract void buildPipeline(StreamExecutionEnvironment env);

    protected abstract String getJobName();
}
