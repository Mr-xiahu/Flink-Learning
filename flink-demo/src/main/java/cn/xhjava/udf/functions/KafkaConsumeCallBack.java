package cn.xhjava.udf.functions;

import cn.xhjava.flink_01_kafka.hander.TaskCallBack;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/27
 * kafka consumer 回调函数,主要用于手动记录offset
 */
public class KafkaConsumeCallBack implements TaskCallBack {
    public KafkaConsumeCallBack(){}

    @Override
    public void callBackUpdateOffset(Map<KafkaTopicPartition, Long> offsets) {

    }
}
