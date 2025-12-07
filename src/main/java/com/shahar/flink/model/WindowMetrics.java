package com.shahar.flink.model;

import com.shahar.bank.Transaction;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable value object representing aggregated metrics for a window of
 * transactions.
 * 
 * DESIGN PATTERN: Value Object
 * - Immutable (all fields final, defensive copying of mutable collections)
 * - Encapsulates related data that belongs together
 * - Type-safe alternative to primitive obsession
 * - Equals/hashCode based on value, not identity
 * 
 * BENEFITS:
 * - Clear contract between aggregation and fraud detection logic
 * - Prevents accidental mutation of aggregated data
 * - Easier to test - can create test instances easily
 * - Self-documenting - method signatures are clearer with WindowMetrics than 5
 * primitives
 * - Serializable for Flink distribution
 */
public final class WindowMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String accountId;
    private final int transactionCount;
    private final double totalAmount;
    private final long windowStart;
    private final long windowEnd;
    private final List<Transaction> transactions;

    /**
     * Private constructor - use builder for construction.
     */
    private WindowMetrics(
            String accountId,
            int transactionCount,
            double totalAmount,
            long windowStart,
            long windowEnd,
            List<Transaction> transactions) {
        this.accountId = accountId;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        // Defensive copy to ensure immutability
        this.transactions = Collections.unmodifiableList(transactions);
    }

    // Getters
    public String getAccountId() {
        return accountId;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public List<Transaction> getTransactions() {
        return transactions; // Already unmodifiable
    }

    /**
     * Calculate window duration in milliseconds.
     */
    public long getWindowDurationMs() {
        return windowEnd - windowStart;
    }

    /**
     * Calculate window duration in minutes.
     */
    public long getWindowDurationMinutes() {
        return getWindowDurationMs() / (60 * 1000);
    }

    /**
     * Calculate average transaction amount.
     */
    public double getAverageTransactionAmount() {
        return transactionCount > 0 ? totalAmount / transactionCount : 0.0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WindowMetrics that = (WindowMetrics) o;
        return transactionCount == that.transactionCount &&
                Double.compare(that.totalAmount, totalAmount) == 0 &&
                windowStart == that.windowStart &&
                windowEnd == that.windowEnd &&
                Objects.equals(accountId, that.accountId) &&
                Objects.equals(transactions, that.transactions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, transactionCount, totalAmount, windowStart, windowEnd, transactions);
    }

    @Override
    public String toString() {
        return String.format(
                "WindowMetrics{accountId='%s', count=%d, total=$%.2f, window=[%d-%d], duration=%dmin}",
                accountId, transactionCount, totalAmount, windowStart, windowEnd, getWindowDurationMinutes());
    }

    /**
     * Builder for WindowMetrics.
     * Provides a fluent API for constructing immutable instances.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String accountId;
        private int transactionCount;
        private double totalAmount;
        private long windowStart;
        private long windowEnd;
        private List<Transaction> transactions;

        private Builder() {
        }

        public Builder accountId(String accountId) {
            this.accountId = accountId;
            return this;
        }

        public Builder transactionCount(int transactionCount) {
            this.transactionCount = transactionCount;
            return this;
        }

        public Builder totalAmount(double totalAmount) {
            this.totalAmount = totalAmount;
            return this;
        }

        public Builder windowStart(long windowStart) {
            this.windowStart = windowStart;
            return this;
        }

        public Builder windowEnd(long windowEnd) {
            this.windowEnd = windowEnd;
            return this;
        }

        public Builder transactions(List<Transaction> transactions) {
            this.transactions = transactions;
            return this;
        }

        public WindowMetrics build() {
            // Validation
            Objects.requireNonNull(accountId, "accountId cannot be null");
            Objects.requireNonNull(transactions, "transactions cannot be null");
            if (transactionCount < 0) {
                throw new IllegalArgumentException("transactionCount cannot be negative");
            }
            if (windowStart >= windowEnd) {
                throw new IllegalArgumentException("windowStart must be before windowEnd");
            }

            return new WindowMetrics(
                    accountId,
                    transactionCount,
                    totalAmount,
                    windowStart,
                    windowEnd,
                    transactions);
        }
    }
}
