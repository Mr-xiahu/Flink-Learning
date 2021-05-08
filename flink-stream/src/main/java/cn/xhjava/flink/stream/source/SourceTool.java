package cn.xhjava.flink.stream.source;

import cn.xhjava.domain.OggMsg;
import cn.xhjava.schema.OggMsgSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/4/20
 */
public class SourceTool {

    public static SourceFunction<String> getKafkaSource(String topic) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.0.113:9092");
        prop.setProperty("group.id", "flink_kafka");
        //prop.setProperty("auto.offset.reset", "earliest");
        SourceFunction<String> kafkaSource = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
        return kafkaSource;
    }

    public static SourceFunction<OggMsg> getOggmsg(String topic) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.0.113:9092");
        prop.setProperty("group.id", "flink_kafka");
        //prop.setProperty("auto.offset.reset", "earliest");
        SourceFunction<OggMsg> kafkaSource = new FlinkKafkaConsumer<>(topic, new OggMsgSchema(), prop);
        return kafkaSource;
    }
}
