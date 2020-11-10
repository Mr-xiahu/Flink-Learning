package cn.xhjava.flink_01_kafka.hander;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class Kafka010Fetcher010Handle<T> extends Kafka010Fetcher<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(Kafka010Fetcher010Handle.class);

    private TaskCallBack taskCallBack;

    public Kafka010Fetcher010Handle(SourceFunction.SourceContext<T> sourceContext, Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic, SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated, ProcessingTimeService processingTimeProvider, long autoWatermarkInterval, ClassLoader userCodeClassLoader, String taskNameWithSubtasks, KafkaDeserializationSchema<T> deserializer, Properties kafkaProperties, long pollTimeout, MetricGroup subtaskMetricGroup, MetricGroup consumerMetricGroup, boolean useMetrics, FlinkConnectorRateLimiter rateLimiter) throws Exception {
        super(sourceContext, assignedPartitionsWithInitialOffsets, watermarksPeriodic, watermarksPunctuated, processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, taskNameWithSubtasks, deserializer, kafkaProperties, pollTimeout, subtaskMetricGroup, consumerMetricGroup, useMetrics, rateLimiter);
    }

    public Kafka010Fetcher010Handle(SourceFunction.SourceContext<T> sourceContext, Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic, SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated, ProcessingTimeService processingTimeProvider, long autoWatermarkInterval, ClassLoader userCodeClassLoader, String taskNameWithSubtasks, KafkaDeserializationSchema<T> deserializer, Properties kafkaProperties, long pollTimeout, MetricGroup subtaskMetricGroup, MetricGroup consumerMetricGroup, boolean useMetrics, FlinkConnectorRateLimiter rateLimiter, TaskCallBack taskCallBack) throws Exception {
        super(sourceContext, assignedPartitionsWithInitialOffsets, watermarksPeriodic, watermarksPunctuated, processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, taskNameWithSubtasks, deserializer, kafkaProperties, pollTimeout, subtaskMetricGroup, consumerMetricGroup, useMetrics, rateLimiter);
        this.taskCallBack = taskCallBack;
    }

    @Override
    protected void doCommitInternalOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets, KafkaCommitCallback commitCallback) throws Exception {
        super.doCommitInternalOffsetsToKafka(offsets, commitCallback);
        for (Map.Entry<KafkaTopicPartition, Long> offset : offsets.entrySet()) {
            LOG.info(String.format("topic:%s,partition:%s,offsets:%s", offset.getKey().getTopic(), offset.getKey().getPartition(), offset.getValue()));
        }
        if (taskCallBack != null) {
            taskCallBack.callBackUpdateOffset(offsets);
        }

    }
}
