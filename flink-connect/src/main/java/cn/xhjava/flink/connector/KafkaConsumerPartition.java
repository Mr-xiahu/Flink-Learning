package cn.xhjava.flink.connector;

import cn.xhjava.flink.connector.kafka.MyFlinkKafkaConsumer;
import cn.xhjava.flink.connector.kafka.TaskCallBack;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * @author Xiahu
 * @create 2021-06-24
 *
 * 测试:flink 从指定分区消费kafka数据
 * 测试结果: 成功
 */
public class KafkaConsumerPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000l);
        env.setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "node4:9092");
        prop.setProperty("group.id", "default_20220218");
        MyFlinkKafkaConsumer<String> kafkaSource = new MyFlinkKafkaConsumer<>("flinksqlstream_20220118", new SimpleStringSchema(), prop, new TaskCallBack() {
            @Override
            public void callBackUpdateOffset(Map<KafkaTopicPartition, Long> offsets) {
                Iterator<Map.Entry<KafkaTopicPartition, Long>> iterator = offsets.entrySet().iterator();
                offsets.forEach(new BiConsumer<KafkaTopicPartition, Long>() {
                    @Override
                    public void accept(KafkaTopicPartition partition, Long offset) {
                        //System.err.println(String.format("Partition: %s , Offset: %s", partition.getPartition(), offset));
                    }
                });
            }
        });
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        Map<KafkaTopicPartition, Long> map = new HashMap<>();
        map.put(new KafkaTopicPartition("flinksqlstream_20220118",0),0l);
        kafkaSource.setStartFromSpecificOffsets(map);


        env.addSource(kafkaSource).printToErr();
        env.execute();
    }
}
