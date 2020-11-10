package cn.xhjava.flink_01_kafka.hander;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public interface TaskCallBack extends Serializable {
    public void callBackUpdateOffset(Map<KafkaTopicPartition, Long> offsets);
}
