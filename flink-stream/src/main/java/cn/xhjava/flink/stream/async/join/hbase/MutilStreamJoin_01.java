package cn.xhjava.flink.stream.async.join.hbase;

import cn.xhjava.domain.Student4;
import cn.xhjava.flink.stream.sink.funcations.HbaseSinkFunction;
import cn.xhjava.flink.stream.source.SourceTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author Xiahu
 * @create 2021/4/20
 * <p>
 * 用Async I/O实现流表与维表Joinpublic
 * 单张表关联查询
 */

class MutilStreamJoin_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        //1.添加数据源
        SourceFunction<String> kafkaSource = SourceTool.getKafkaSource("flink_kafka_source");
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        //2.转换为POJO流
        DataStream<Student4> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new Student4(fields[0], fields[1], fields[2]);
        });


        //3.流关联
        HbaseAsyncFunction hbaseAsyncFunction = new HbaseAsyncFunction("lookup:realtime_dim_1");
        DataStream<Student4> resultStream = AsyncDataStream.orderedWait(mapStream, hbaseAsyncFunction, 500, TimeUnit.MILLISECONDS, 1000);

        //resultStream.printToErr();


        HbaseSinkFunction sinkFunction = new HbaseSinkFunction("sink:fink_api_sink_1");
        resultStream.addSink(sinkFunction);

        env.execute();


    }
}
