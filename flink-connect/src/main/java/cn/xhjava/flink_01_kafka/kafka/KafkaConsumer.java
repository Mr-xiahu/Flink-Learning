package cn.xhjava.flink_01_kafka.kafka;

import cn.xhjava.flink_01_kafka.hander.MyFlinkKafkaConsumer;
import cn.xhjava.flink_01_kafka.hander.TaskCallBack;
import cn.xhjava.kafka.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class KafkaConsumer {

    private Map<String, String> config = null;
    private List<String> topics = null;
    private Properties prop = null;


    public KafkaConsumer(ParameterTool parameterTool) {
        prop = parameterTool.getProperties();
        topics = KafkaUtil.getKafkaConsumerTopics(parameterTool);
    }

    public MyFlinkKafkaConsumer<String> buildFlinkKafkaConsumer(ParameterTool parameterTool) {
        prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.113:9092");
        prop.put("group.id","test");
        MyFlinkKafkaConsumer<String> consumer = new MyFlinkKafkaConsumer<String>(topics, new SimpleStringSchema(), prop);
        consumer.setCommitOffsetsOnCheckpoints(true);
        return consumer;
    }

    public FlinkKafkaConsumer011<String> buildFlinkKafkaConsumer2(ParameterTool parameterTool) {
        prop = new Properties();
        prop.put("bootstrap.servers","192.168.0.113:9092");
        prop.put("group.id","test");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(topics, new SimpleStringSchema(), prop);
        consumer.setCommitOffsetsOnCheckpoints(true);
        return consumer;
    }

    public MyFlinkKafkaConsumer<String> buildFlinkKafkaConsumer(ParameterTool parameterTool, TaskCallBack taskCallBack) {
        MyFlinkKafkaConsumer<String> consumer = new MyFlinkKafkaConsumer<String>(topics, new SimpleStringSchema(), prop, taskCallBack);
        consumer.setCommitOffsetsOnCheckpoints(true);
        return consumer;
    }
}
