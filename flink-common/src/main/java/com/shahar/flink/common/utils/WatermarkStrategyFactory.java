package com.shahar.flink.common.utils;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public class WatermarkStrategyFactory {

    public static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(
            Duration maxOutOfOrderness,
            SerializableTimestampAssigner<T> timestampAssigner) {
        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner(timestampAssigner);
    }

    public static <T> WatermarkStrategy<T> forMonotonousTimestamps(
            SerializableTimestampAssigner<T> timestampAssigner) {
        return WatermarkStrategy
                .<T>forMonotonousTimestamps()
                .withTimestampAssigner(timestampAssigner);
    }
}
