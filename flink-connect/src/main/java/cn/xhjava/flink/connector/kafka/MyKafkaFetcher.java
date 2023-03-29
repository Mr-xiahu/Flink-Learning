package cn.xhjava.flink.connector.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.*;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021-06-24
 */
public class MyKafkaFetcher<T> extends KafkaFetcher<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaFetcher.class);

    private TaskCallBack taskCallBack;

    public MyKafkaFetcher(SourceFunction.SourceContext<T> sourceContext, Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets, SerializedValue<WatermarkStrategy<T>> watermarkStrategy, ProcessingTimeService processingTimeProvider, long autoWatermarkInterval, ClassLoader userCodeClassLoader, String taskNameWithSubtasks, KafkaDeserializationSchema<T> deserializer, Properties kafkaProperties, long pollTimeout, MetricGroup subtaskMetricGroup, MetricGroup consumerMetricGroup, boolean useMetrics) throws Exception {
        super(sourceContext, assignedPartitionsWithInitialOffsets, watermarkStrategy, processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, taskNameWithSubtasks, deserializer, kafkaProperties, pollTimeout, subtaskMetricGroup, consumerMetricGroup, useMetrics);
    }

    public MyKafkaFetcher(SourceFunction.SourceContext<T> sourceContext, Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets, SerializedValue<WatermarkStrategy<T>> watermarkStrategy, ProcessingTimeService processingTimeProvider, long autoWatermarkInterval, ClassLoader userCodeClassLoader, String taskNameWithSubtasks, KafkaDeserializationSchema<T> deserializer, Properties kafkaProperties, long pollTimeout, MetricGroup subtaskMetricGroup, MetricGroup consumerMetricGroup, boolean useMetrics, TaskCallBack taskCallBack) throws Exception {
        super(sourceContext, assignedPartitionsWithInitialOffsets, watermarkStrategy, processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, taskNameWithSubtasks, deserializer, kafkaProperties, pollTimeout, subtaskMetricGroup, consumerMetricGroup, useMetrics);
        this.taskCallBack = taskCallBack;
    }

    //重写改方法,此方法在执行时会提交offset
    @Override
    protected void doCommitInternalOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets, KafkaCommitCallback commitCallback) throws Exception {
        super.doCommitInternalOffsetsToKafka(offsets, commitCallback);
        if (taskCallBack != null) {
            taskCallBack.callBackUpdateOffset(offsets);
        }
    }
}
