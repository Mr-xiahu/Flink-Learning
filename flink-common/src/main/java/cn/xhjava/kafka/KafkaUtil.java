package cn.xhjava.kafka;

import cn.xhjava.constant.FlinkLearnConstant;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class KafkaUtil {
    public static Properties getKafkaProp(ParameterTool parameterTool) {
        return parameterTool.getProperties();
    }

    public static List<String> getKafkaConsumerTopics(ParameterTool parameterTool) {
        List<String> list = new ArrayList<>();
        for (String topic : parameterTool.get(FlinkLearnConstant.FLINK_KAFKA_TOPIC).split(",")) {
            list.add(topic);
        }
        return list;
    }

    public static String getKafkaProduceTopic(ParameterTool parameterTool) {
        return parameterTool.get(FlinkLearnConstant.FLINK_KAFKA_produce_TOPIC);
    }

    public static String getKafkaProduceBrokers(ParameterTool parameterTool) {
        return parameterTool.get(FlinkLearnConstant.FLINK_KAFKA_produce_BROKER);
    }
}
