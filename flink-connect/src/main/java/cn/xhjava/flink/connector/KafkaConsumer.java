package cn.xhjava.flink.connector;

import cn.xhjava.flink.connector.kafka.MyFlinkKafkaConsumer;
import cn.xhjava.flink.connector.kafka.TaskCallBack;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * @author Xiahu
 * @create 2021-06-24
 */
//Flink 消费kafka消息,回调函数提交offset
public class KafkaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000l);
        env.setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "node4:9092");
        prop.setProperty("group.id", "default");
        MyFlinkKafkaConsumer<String> kafkaSource = new MyFlinkKafkaConsumer<>("xh_mor", new SimpleStringSchema(), prop, new TaskCallBack() {
            @Override
            public void callBackUpdateOffset(Map<KafkaTopicPartition, Long> offsets) {
                Iterator<Map.Entry<KafkaTopicPartition, Long>> iterator = offsets.entrySet().iterator();
                offsets.forEach(new BiConsumer<KafkaTopicPartition, Long>() {
                    @Override
                    public void accept(KafkaTopicPartition partition, Long offset) {
                        System.err.println(String.format("Partition: %s , Offset: %s", partition.getPartition(), offset));
                    }
                });
            }
        });
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        env.addSource(kafkaSource).print();
        env.execute();
    }
}
