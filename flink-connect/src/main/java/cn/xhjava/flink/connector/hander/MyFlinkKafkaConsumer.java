package cn.xhjava.flink.connector.hander;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class MyFlinkKafkaConsumer<T> extends FlinkKafkaConsumer010<T> implements Serializable {
    private TaskCallBack taskCallBack;


    public MyFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        this(Collections.singletonList(topic), valueDeserializer, props);
    }

    public MyFlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> valueDeserializer, Properties props, TaskCallBack callBack) {
        this((List) topics, valueDeserializer, props);
        this.taskCallBack = callBack;
    }


    public MyFlinkKafkaConsumer(String topic, DeserializationSchema<T> deserializer, Properties props, TaskCallBack callBack) {
        this(Collections.singletonList(topic), deserializer, props);
        this.taskCallBack = callBack;
    }

    public MyFlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        this((List) topics, (KafkaDeserializationSchema) (new KafkaDeserializationSchemaWrapper(deserializer)), props);
    }

    public MyFlinkKafkaConsumer(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(topics, deserializer, props);
    }

    @PublicEvolving
    public MyFlinkKafkaConsumer(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
        this((Pattern) subscriptionPattern, (KafkaDeserializationSchema) (new KafkaDeserializationSchemaWrapper(valueDeserializer)), props);
    }

    @PublicEvolving
    public MyFlinkKafkaConsumer(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(subscriptionPattern, deserializer, props);
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext, Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic, SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated, StreamingRuntimeContext runtimeContext, OffsetCommitMode offsetCommitMode, MetricGroup consumerMetricGroup, boolean useMetrics) throws Exception {

        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
            this.properties.setProperty("enable.auto.commit", "false");
        }

        FlinkConnectorRateLimiter rateLimiter = super.getRateLimiter();
        if (rateLimiter != null) {
            rateLimiter.open(runtimeContext);
        }

        if (this.taskCallBack == null) {
            return new Kafka010Fetcher010Handle(sourceContext, assignedPartitionsWithInitialOffsets, watermarksPeriodic, watermarksPunctuated, runtimeContext.getProcessingTimeService(), runtimeContext.getExecutionConfig().getAutoWatermarkInterval(), runtimeContext.getUserCodeClassLoader(), runtimeContext.getTaskNameWithSubtasks(), this.deserializer, this.properties, this.pollTimeout, runtimeContext.getMetricGroup(), consumerMetricGroup, useMetrics, rateLimiter);
        } else {
            return new Kafka010Fetcher010Handle(sourceContext, assignedPartitionsWithInitialOffsets, watermarksPeriodic, watermarksPunctuated, runtimeContext.getProcessingTimeService(), runtimeContext.getExecutionConfig().getAutoWatermarkInterval(), runtimeContext.getUserCodeClassLoader(), runtimeContext.getTaskNameWithSubtasks(), this.deserializer, this.properties, this.pollTimeout, runtimeContext.getMetricGroup(), consumerMetricGroup, useMetrics, rateLimiter, taskCallBack);
        }

    }
}
