package cn.xhjava.flink.connector.hander;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class MyFlinkKafkaConsumer<T> extends FlinkKafkaConsumer<T> implements Serializable {
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
}
