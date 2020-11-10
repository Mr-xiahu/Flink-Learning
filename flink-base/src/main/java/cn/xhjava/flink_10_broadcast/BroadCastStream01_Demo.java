package cn.xhjava.flink_10_broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.xml.MetaData;

import java.util.List;

/**
 * @author Xiahu
 * @create 2020/11/10
 * 广播流使用
 */
public class BroadCastStream01_Demo {
    public static void main(String[] args) {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //加载mysql数据库中的流
        DataStreamSource<List<MetaData>> broadcastStream = env.addSource(new GetTableMetadataSource());
        MapStateDescriptor<String, MetaData> metadata = new MapStateDescriptor<>(
                "metadata",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(MetaData.class));
        //模拟真实数据流
        DataStreamSource<String> kafkaStrean = env.fromElements("{\"table\":\"xh.test\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\",\"current_ts\":\"2020-05-07T16:25:34.000290\",\"pos\":\"04172325706511144646\"," +
                "\"primary_keys\":[\"id\"],\"after\":{\"id\":\"94\",\"fk_id\":\"2010\",\"qfxh\":\"94\",\"jdpj\":\"AFLWAI\",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\",\"isdeleted\":\"0\",\"lastupdatedttm\":\"2020-05-07 16:25:34.290\",\"rowkey\":\"94\"}}");


        kafkaStrean.connect(broadcastStream.broadcast(metadata)).process(new MyBroadCastProcessFuncation()).printToErr();


    }
}
