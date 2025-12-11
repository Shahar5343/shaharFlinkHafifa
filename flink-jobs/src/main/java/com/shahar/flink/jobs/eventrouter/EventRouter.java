package com.shahar.flink.jobs.eventrouter;

import com.shahar.bank.BankEvent;
import com.shahar.bank.EventType;
import com.shahar.bank.FraudReport;
import com.shahar.bank.Transaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventRouter extends ProcessFunction<BankEvent, Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(EventRouter.class);

    public static final OutputTag<FraudReport> FRAUD_REPORT_TAG = new OutputTag<FraudReport>("fraud-reports") {
    };

    @Override
    public void processElement(
            BankEvent event,
            Context ctx,
            Collector<Transaction> out) throws Exception {

        if (event == null || event.getMetadata() == null) {
            LOG.warn("Received null event or metadata, skipping");
            return;
        }

        EventType eventType = event.getMetadata().getEventType();

        switch (eventType) {
            case Transaction:
                if (event.getBody() instanceof Transaction) {
                    Transaction transaction = (Transaction) event.getBody();
                    out.collect(transaction);
                    LOG.debug("Routed Transaction: {}", transaction.getId());
                } else {
                    LOG.error("Event type is Transaction but body is not: {}", event.getBody().getClass());
                }
                break;

            case FraudReport:
                if (event.getBody() instanceof FraudReport) {
                    FraudReport fraudReport = (FraudReport) event.getBody();
                    ctx.output(FRAUD_REPORT_TAG, fraudReport);
                    LOG.debug("Routed FraudReport: {}", fraudReport.getReportId());
                } else {
                    LOG.error("Event type is FraudReport but body is not: {}", event.getBody().getClass());
                }
                break;

            default:
                LOG.warn("Unknown event type: {}", eventType);
        }
    }
}
