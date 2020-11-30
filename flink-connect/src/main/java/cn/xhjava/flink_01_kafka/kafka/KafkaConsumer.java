package cn.xhjava.flink_01_kafka.kafka;

import cn.xhjava.constant.FlinkLearnConstant;
import cn.xhjava.domain.AlertEvent;
import cn.xhjava.domain.OggMsg;
import cn.xhjava.flink_01_kafka.hander.MyFlinkKafkaConsumer;
import cn.xhjava.flink_01_kafka.hander.TaskCallBack;
import cn.xhjava.kafka.KafkaUtil;
import cn.xhjava.schema.AlertEventSchema;
import cn.xhjava.schema.OggMsgSchema;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import cn.xhjava.constant.FlinkLearnConstant.*;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class KafkaConsumer {
    private static final String BROKER = "bootstrap.servers";
    private static final String GROUP_ID = "group.id";

    private List<String> topics = null;
    private Properties prop = null;
    public ParameterTool parameter;


    public KafkaConsumer(ParameterTool parameterTool) {
        this.parameter = parameterTool;
        topics = KafkaUtil.getKafkaConsumerTopics(parameterTool);
    }

    public MyFlinkKafkaConsumer<String> buildFlinkKafkaConsumer() {
        prop = new Properties();
        prop.put(BROKER, parameter.get(FlinkLearnConstant.FLINK_KAFKA_BROKERS));
        prop.put(GROUP_ID, parameter.get(FlinkLearnConstant.FLINK_KAFKA_GROUP_ID));
        MyFlinkKafkaConsumer<String> consumer = new MyFlinkKafkaConsumer<String>(topics, new SimpleStringSchema(), prop);
        consumer.setCommitOffsetsOnCheckpoints(true);
        return consumer;
    }


    public MyFlinkKafkaConsumer<String> buildFlinkKafkaConsumer(TaskCallBack taskCallBack) {
        prop = new Properties();
        prop.put(BROKER, parameter.get(FlinkLearnConstant.FLINK_KAFKA_BROKERS));
        prop.put(GROUP_ID, parameter.get(FlinkLearnConstant.FLINK_KAFKA_GROUP_ID));
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        MyFlinkKafkaConsumer<String> consumer  = new MyFlinkKafkaConsumer<String>(topics, new SimpleStringSchema(), prop, taskCallBack);
        consumer.setCommitOffsetsOnCheckpoints(true);
        return consumer;
    }

    public MyFlinkKafkaConsumer<OggMsg> buildFlinkKafkaConsumerOggMsg(TaskCallBack taskCallBack) {
        prop = new Properties();
        prop.put(BROKER, parameter.get(FlinkLearnConstant.FLINK_KAFKA_BROKERS));
        prop.put(GROUP_ID, parameter.get(FlinkLearnConstant.FLINK_KAFKA_GROUP_ID));
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        MyFlinkKafkaConsumer<OggMsg> consumer = new MyFlinkKafkaConsumer<OggMsg>(topics, new OggMsgSchema(), prop, taskCallBack);
        consumer.setCommitOffsetsOnCheckpoints(true);
        return consumer;
    }
}
