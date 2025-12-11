package com.shahar.flink.jobs.eventrouter;

import com.shahar.flink.common.base.BaseConfig;

public class EventRouterConfig extends BaseConfig {

    private static EventRouterConfig instance;

    private EventRouterConfig() {
        super("application.properties");
    }

    public static synchronized EventRouterConfig getInstance() {
        if (instance == null) {
            instance = new EventRouterConfig();
        }
        return instance;
    }

    public String getInputTopic() {
        return getProperty("kafka.input.topic", "bank-event");
    }

    public String getTransactionTopic() {
        return getProperty("kafka.transaction.topic", "transactions");
    }

    public String getFraudReportTopic() {
        return getProperty("kafka.fraud-report.topic", "fraud-reports");
    }

    @Override
    public String getConsumerGroupId() {
        return getProperty("kafka.consumer.group.id", "event-router-consumer");
    }
}
