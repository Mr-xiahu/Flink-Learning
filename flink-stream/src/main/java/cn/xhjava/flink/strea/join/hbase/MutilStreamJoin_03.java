package cn.xhjava.flink.strea.join.hbase;

import cn.xhjava.flink.stream.pojo.Student4;
import cn.xhjava.flink.stream.sink.funcations.HbaseSinkFunction;
import cn.xhjava.flink.stream.source.SourceTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Xiahu
 * @create 2021/4/20
 * <p>
 * window + function
 */

class MutilStreamJoin_03 {
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

        MyHbaseProcessAllWindowFunction processFunction = new MyHbaseProcessAllWindowFunction("lookup:realtime_dim_1,lookup:realtime_dim_2,lookup:realtime_dim_3," +
                "lookup:realtime_dim_4,lookup:realtime_dim_5");
        /* MyRedisProcessAllWindowFunction processFunction = new MyRedisProcessAllWindowFunction("lookup:realtime_dim_1");*/

        DataStream<Student4> process = mapStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(processFunction);


        //process.printToErr();

        HbaseSinkFunction sinkFunction = new HbaseSinkFunction("sink:fink_api_sink_1");
        process.addSink(sinkFunction);
        env.execute();


    }
}
