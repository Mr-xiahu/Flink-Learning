package cn.xhjava.flink.stream.main;

import cn.xhjava.domain.OggMsg;
import cn.xhjava.flink.stream.sink.RedisClusterSink;
import cn.xhjava.flink.stream.source.SourceTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/5/8
 * <p>
 * 消费kafka消息,落地到redis 集群
 */
public class KafkaToRedisCluster {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        Properties properties = new Properties();
        properties.load(new FileReader(new File("D:\\code\\github\\Flink-Learning\\flink-stream\\src\\main\\resources\\application.properties")));
        //properties.load(new FileReader(new File(args[0])));

        //1.添加数据源
        SourceFunction<OggMsg> kafkaSource = SourceTool.getOggmsg("kafka_redis2");
        DataStreamSource<OggMsg> dataStream = env.addSource(kafkaSource);

        dataStream.addSink(new RedisClusterSink(properties, columnList()));

        env.execute();
    }


    //模拟获取gp 数据库内column,生产环境下可能从元数据库内获取
    public static LinkedList<String> columnList() {
        LinkedList<String> columnList = new LinkedList<>();
        columnList.addLast("id");
        columnList.addLast("fk_id");
        columnList.addLast("qfxh");
        columnList.addLast("jdpj");
        columnList.addLast("nioroa");
        columnList.addLast("gwvz");
        columnList.addLast("joqtf");
        columnList.addLast("isdeleted");
        columnList.addLast("lastupdatedttm");
        columnList.addLast("rowkey");
        return columnList;
    }
}
