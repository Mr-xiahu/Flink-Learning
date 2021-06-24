package cn.xhjava.flink.connector.kafka;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/2
 */

// FlinkKafkaConsumer 提交成功后回调方法
public interface TaskCallBack extends Serializable {
    public void callBackUpdateOffset(Map<KafkaTopicPartition, Long> offsets);
}
