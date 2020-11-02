package cn.xhjava.kafka;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class KafkaUtil {
    public static Properties getKafkaProp(ParameterTool parameterTool) {
        return parameterTool.getProperties();
    }
}
