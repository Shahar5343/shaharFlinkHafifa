package com.shahar.flink.functions;

import com.shahar.bank.Transaction;
import com.shahar.flink.Alert;
import com.shahar.flink.config.FlinkConfig;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * ==============================================================================
 * WHAT: Windowed fraud detection logic for transaction streams
 * WHY: Detects suspicious patterns by aggregating transactions per account
 * HOW: Uses ProcessWindowFunction to access all transactions in a window
 * ==============================================================================
 * 
 * LEARNING POINT: ProcessWindowFunction vs AggregateFunction
 * - ProcessWindowFunction: Access to ALL elements in window + window metadata
 * - AggregateFunction: Incremental aggregation (more memory efficient)
 * 
 * We use ProcessWindowFunction because we need:
 * 1. Window start/end times for the alert
 * 2. Complex logic that examines multiple fields
 * 3. Ability to generate multiple alerts per window (if needed)
 * 
 * Trade-off: Higher memory usage (stores all elements) but more flexibility
 */
public class FraudDetector extends ProcessWindowFunction<Transaction, Alert, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);

    private final FlinkConfig config;

    public FraudDetector() {
        this.config = FlinkConfig.getInstance();
    }

    /**
     * WHAT: Process all transactions in a window for a single account
     * WHY: Detect fraud patterns across multiple transactions
     * HOW: Aggregate metrics and apply rules to generate alerts
     * 
     * @param accountId    Key (account ID) for this window
     * @param context      Window context (provides window metadata)
     * @param transactions All transactions in this window for this account
     * @param out          Collector for output alerts
     * 
     *                     LEARNING POINT: Window lifecycle
     *                     1. Events arrive with timestamps
     *                     2. Flink assigns to windows based on event time
     *                     3. Watermark advances (event_time - max_out_of_orderness)
     *                     4. When watermark passes window end, this function is
     *                     called
     *                     5. Window state is cleared after processing
     */
    @Override
    public void process(
            String accountId,
            Context context,
            Iterable<Transaction> transactions,
            Collector<Alert> out) throws Exception {

        TimeWindow window = context.window();

        // ====================================================================
        // STEP 1: Aggregate transaction metrics
        // ====================================================================
        // LEARNING POINT: We're doing this manually because we need multiple
        // metrics. In production, you might use a more sophisticated state
        // management approach for very large windows.

        int transactionCount = 0;
        double totalAmount = 0.0;
        List<Transaction> transactionList = new ArrayList<>();

        for (Transaction transaction : transactions) {
            transactionCount++;
            totalAmount += transaction.getAmountTransferred();
            transactionList.add(transaction);
        }

        LOG.debug("Processing window for account {}: {} transactions, total amount: {}",
                accountId, transactionCount, totalAmount);

        // ====================================================================
        // STEP 2: Apply fraud detection rules
        // ====================================================================
        // LEARNING POINT: These are simple rule-based detections.
        // In production, you might use:
        // - Machine learning models (Flink ML)
        // - Complex event processing (CEP)
        // - External service calls (async I/O)
        // - Historical pattern comparison (state backend)

        Alert alert = detectFraud(
                accountId,
                transactionCount,
                totalAmount,
                window.getStart(),
                window.getEnd(),
                transactionList);

        // ====================================================================
        // STEP 3: Emit alert if fraud detected
        // ====================================================================
        if (alert != null) {
            out.collect(alert);
            LOG.info("FRAUD ALERT: Account {} - Urgency {} - {}",
                    accountId, alert.getUrgencyLevel(), alert.getReason());
        }
    }

    /**
     * WHAT: Apply fraud detection rules and generate alert
     * WHY: Encapsulates business logic for fraud detection
     * HOW: Checks multiple conditions and determines urgency level
     * 
     * @return Alert if fraud detected, null otherwise
     * 
     *         LEARNING POINT: Fraud detection rules (from requirements)
     *         - Rule 1: > 10 transactions in window → Medium urgency (2)
     *         - Rule 2: Total amount > $10,000 → High urgency (3)
     *         - Rule 3: Both conditions → Critical urgency (4)
     * 
     *         In production, these thresholds would be:
     *         - Configurable per account type
     *         - Adjusted based on historical patterns
     *         - Learned from ML models
     */
    private Alert detectFraud(
            String accountId,
            int transactionCount,
            double totalAmount,
            long windowStart,
            long windowEnd,
            List<Transaction> transactions) {

        // Get thresholds from configuration
        int countThreshold = config.getFraudThresholdTransactionCount();
        double amountThreshold = config.getFraudThresholdTotalAmount();

        // Check if any fraud rules are triggered
        boolean highFrequency = transactionCount > countThreshold;
        boolean largeAmount = totalAmount > amountThreshold;

        if (!highFrequency && !largeAmount) {
            // No fraud detected
            return null;
        }

        // Determine urgency level and reason
        int urgencyLevel;
        String reason;
        int alertTypeId;

        if (highFrequency && largeAmount) {
            // CRITICAL: Both rules triggered
            urgencyLevel = 4;
            alertTypeId = 3;
            reason = String.format(
                    "CRITICAL: High frequency (%d transactions) AND large amount ($%.2f) detected in 5-minute window",
                    transactionCount, totalAmount);
        } else if (largeAmount) {
            // HIGH: Large amount only
            urgencyLevel = 3;
            alertTypeId = 2;
            reason = String.format(
                    "HIGH: Large total amount ($%.2f) detected in 5-minute window",
                    totalAmount);
        } else {
            // MEDIUM: High frequency only
            urgencyLevel = 2;
            alertTypeId = 1;
            reason = String.format(
                    "MEDIUM: High transaction frequency (%d transactions) detected in 5-minute window",
                    transactionCount);
        }

        // Build alert using Avro builder pattern
        // LEARNING POINT: Avro-generated classes provide builder pattern
        // for clean, type-safe object construction
        return Alert.newBuilder()
                .setAccountId(accountId)
                .setTypeId(alertTypeId)
                .setReason(reason)
                .setUrgencyLevel(urgencyLevel)
                .setWindowStart(windowStart)
                .setWindowEnd(windowEnd)
                .setTransactionCount(transactionCount)
                .setTotalAmount(totalAmount)
                .build();
    }
}
