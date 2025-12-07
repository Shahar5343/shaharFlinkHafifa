package com.shahar.flink.functions;

import com.shahar.bank.BankEvent;
import com.shahar.bank.EventType;
import com.shahar.bank.FraudReport;
import com.shahar.bank.Transaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ==============================================================================
 * WHAT: Routes polymorphic BankEvent to type-specific streams
 * WHY: Enables different processing logic for Transactions vs FraudReports
 * HOW: Uses Flink side outputs to split stream based on event type
 * ==============================================================================
 * 
 * LEARNING POINT: This demonstrates the "side output" pattern in Flink.
 * Instead of using filter() which creates multiple passes over the data,
 * side outputs allow a single pass with type-safe routing.
 * 
 * Pattern:
 * 1. Single input stream (BankEvent)
 * 2. ProcessFunction inspects metadata.event_type
 * 3. Emits to different OutputTags based on type
 * 4. Downstream operators access specific streams via getSideOutput()
 */
public class EventRouter extends ProcessFunction<BankEvent, Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(EventRouter.class);

    /**
     * WHAT: OutputTag for FraudReport events
     * WHY: Type-safe way to identify side output streams
     * LEARNING POINT: OutputTags must be static final to ensure they're
     * the same instance when retrieving the side output later
     */
    public static final OutputTag<FraudReport> FRAUD_REPORT_TAG = new OutputTag<FraudReport>("fraud-reports") {
    };

    /**
     * WHAT: Process each BankEvent and route to appropriate stream
     * WHY: Single-pass routing is more efficient than multiple filters
     * HOW: Check event type and emit to main output or side output
     * 
     * @param event Input BankEvent (polymorphic wrapper)
     * @param ctx   ProcessFunction context (provides access to side outputs)
     * @param out   Main output collector (for Transaction events)
     * 
     *              LEARNING POINT: Main output vs Side output
     *              - Main output (out.collect): Primary stream, used for most
     *              common type
     *              - Side output (ctx.output): Secondary streams, accessed via tags
     */
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

        // Route based on event type
        switch (eventType) {
            case Transaction:
                // LEARNING POINT: Avro union types
                // The body field is an Avro union [Transaction, FraudReport]
                // We need to check the actual type before casting
                if (event.getBody() instanceof Transaction) {
                    Transaction transaction = (Transaction) event.getBody();
                    out.collect(transaction); // Emit to main output
                    LOG.debug("Routed Transaction: {}", transaction.getId());
                } else {
                    LOG.error("Event type is Transaction but body is not: {}",
                            event.getBody().getClass());
                }
                break;

            case FraudReport:
                if (event.getBody() instanceof FraudReport) {
                    FraudReport fraudReport = (FraudReport) event.getBody();
                    ctx.output(FRAUD_REPORT_TAG, fraudReport); // Emit to side output
                    LOG.debug("Routed FraudReport: {}", fraudReport.getReportId());
                } else {
                    LOG.error("Event type is FraudReport but body is not: {}",
                            event.getBody().getClass());
                }
                break;

            default:
                LOG.warn("Unknown event type: {}", eventType);
        }
    }
}
