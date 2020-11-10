package cn.xhjava.flink_01_kafka.kafka;

import cn.xhjava.constant.KafkaConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author Xiahu
 * @create 2020/11/4
 */
public class KafkaProducer {

    public FlinkKafkaProducer011 buildFlinkKafkaConsumer(ParameterTool parameterTool) {
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(
                parameterTool.get(KafkaConstant.KAFKA_PRODUCE_BOOTSTRAP_SERVERS),
                parameterTool.get(KafkaConstant.KAFKA_PRODUCE_TOPICS),
                new SimpleStringSchema());
        producer.setWriteTimestampToKafka(true);
        return producer;
    }
}
