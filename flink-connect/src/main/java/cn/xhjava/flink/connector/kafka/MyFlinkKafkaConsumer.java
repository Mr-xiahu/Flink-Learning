package cn.xhjava.flink.connector.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.*;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getBoolean;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * @author Xiahu
 * @create 2021-06-24
 */
public class MyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {

    protected Properties properties;

    protected long pollTimeout;

    public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";

    public static final long DEFAULT_POLL_TIMEOUT = 100L;

    private TaskCallBack taskCallBack;

    public MyFlinkKafkaConsumer(List<String> topics, Pattern topicPattern, KafkaDeserializationSchema<T> deserializer, long discoveryIntervalMillis, boolean useMetrics) {
        super(topics, topicPattern, deserializer, discoveryIntervalMillis, useMetrics);
    }

    public MyFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props,TaskCallBack taskCallBack) {
        this(Collections.singletonList(topic), valueDeserializer, props);
        this.taskCallBack = taskCallBack;
    }


    public MyFlinkKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        this(Collections.singletonList(topic), deserializer, props);
    }

    public MyFlinkKafkaConsumer(String topic, KafkaDeserializationSchema<T> deserializer, Properties props, TaskCallBack taskCallBack) {
        this(Collections.singletonList(topic), deserializer, props);
        this.taskCallBack = taskCallBack;
    }


    public MyFlinkKafkaConsumer(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        this(topics, new KafkaDeserializationSchemaWrapper<>(deserializer), props);
    }


    public MyFlinkKafkaConsumer(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
        this(topics, null, deserializer, props);
    }

    private MyFlinkKafkaConsumer(
            List<String> topics,
            Pattern subscriptionPattern,
            KafkaDeserializationSchema<T> deserializer,
            Properties props) {

        super(topics, subscriptionPattern, deserializer,
                getLong(
                        checkNotNull(props, "props"),
                        KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                        PARTITION_DISCOVERY_DISABLED),
                !getBoolean(props, KEY_DISABLE_METRICS, false));

        this.properties = props;
        setDeserializer(this.properties);

        // configure the polling timeout
        try {
            if (properties.containsKey(KEY_POLL_TIMEOUT)) {
                this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
            } else {
                this.pollTimeout = DEFAULT_POLL_TIMEOUT;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
        }
    }


    //回调方法(主要)
    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {
        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
            this.properties.setProperty("enable.auto.commit", "false");
        }

        if (taskCallBack != null) {
            return new MyKafkaFetcher<>(
                    sourceContext,
                    assignedPartitionsWithInitialOffsets,
                    watermarkStrategy,
                    runtimeContext.getProcessingTimeService(),
                    runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                    runtimeContext.getUserCodeClassLoader(),
                    runtimeContext.getTaskNameWithSubtasks(),
                    deserializer,
                    properties,
                    pollTimeout,
                    runtimeContext.getMetricGroup(),
                    consumerMetricGroup,
                    useMetrics,
                    taskCallBack);
        } else {
            return new MyKafkaFetcher<>(
                    sourceContext,
                    assignedPartitionsWithInitialOffsets,
                    watermarkStrategy,
                    runtimeContext.getProcessingTimeService(),
                    runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                    runtimeContext.getUserCodeClassLoader(),
                    runtimeContext.getTaskNameWithSubtasks(),
                    deserializer,
                    properties,
                    pollTimeout,
                    runtimeContext.getMetricGroup(),
                    consumerMetricGroup,
                    useMetrics);
        }
    }


    @Override
    protected AbstractPartitionDiscoverer createPartitionDiscoverer(KafkaTopicsDescriptor topicsDescriptor, int indexOfThisSubtask, int numParallelSubtasks) {
        return new KafkaPartitionDiscoverer(
                topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, properties);
    }

    @Override
    protected boolean getIsAutoCommitEnabled() {
        return getBoolean(properties, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                && PropertiesUtil.getLong(
                properties, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000)
                > 0;
    }

    @Override
    protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(Collection<KafkaTopicPartition> partitions, long timestamp) {
        Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
        for (KafkaTopicPartition partition : partitions) {
            partitionOffsetsRequest.put(
                    new TopicPartition(partition.getTopic(), partition.getPartition()), timestamp);
        }

        final Map<KafkaTopicPartition, Long> result = new HashMap<>(partitions.size());
        // use a short-lived consumer to fetch the offsets;
        // this is ok because this is a one-time operation that happens only on startup
        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer(properties)) {
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
                    consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

                result.put(
                        new KafkaTopicPartition(
                                partitionToOffset.getKey().topic(),
                                partitionToOffset.getKey().partition()),
                        (partitionToOffset.getValue() == null)
                                ? null
                                : partitionToOffset.getValue().offset());
            }
        }
        return result;
    }

    private static void setDeserializer(Properties props) {
        final String deSerName = ByteArrayDeserializer.class.getName();

        Object keyDeSer = props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        Object valDeSer = props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        if (keyDeSer != null && !keyDeSer.equals(deSerName)) {
            LOG.warn(
                    "Ignoring configured key DeSerializer ({})",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }
        if (valDeSer != null && !valDeSer.equals(deSerName)) {
            LOG.warn(
                    "Ignoring configured value DeSerializer ({})",
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deSerName);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deSerName);
    }
}
