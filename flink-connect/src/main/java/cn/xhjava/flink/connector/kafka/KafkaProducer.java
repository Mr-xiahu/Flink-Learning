package cn.xhjava.flink.connector.kafka;

import cn.xhjava.constant.FlinkLearnConstant;
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
                parameterTool.get(FlinkLearnConstant.FLINK_KAFKA_produce_BROKER),
                parameterTool.get(FlinkLearnConstant.FLINK_KAFKA_produce_TOPIC),
                new SimpleStringSchema());
//        producer.setWriteTimestampToKafka(true);
        return producer;
    }
}
