package cn.xhjava.flink.stream.main.redis;

import cn.xhjava.domain.Student5;
import cn.xhjava.flink.stream.sink.FileSink;
import cn.xhjava.flink.stream.source.SourceTool;
import cn.xhjava.flink.stream.transfromfunction.MyRedisProcessAllWindowFunction2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

/**
 * @author Xiahu
 * @create 2021/4/20
 * <p>
 * TumblingEventTimeWindows + function + redis + 单线程
 * EventTime 不被使用
 */

class MutilStreamJoin_Redis_06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);
        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://master:8020/tmp/flink/redis");
        env.setStateBackend(fsStateBackend);

        //1.添加数据源
        SourceFunction<String> kafkaSource = SourceTool.getKafkaSource("flink_kafka_source");
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        //2.转换为POJO流
        DataStream<Student5> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            Student5 student5 = new Student5(fields[0], fields[1], fields[2], Long.valueOf(fields[3]));
            return student5;
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy(new BoundedOutOfOrdernessTimestampExtractor<Student5>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Student5 element) {
                return element.getCurrentTime();
            }
        }));


        MyRedisProcessAllWindowFunction2 processFunction = new MyRedisProcessAllWindowFunction2("redis_test_1,redis_test_2,redis_test_3,redis_test_4,redis_test_5,redis_test_6,redis_test_7,redis_test_8,redis_test_9,redis_test_10,redis_test_11,redis_test_12," +
                "redis_test_13,redis_test_14,redis_test_15,redis_test_16,redis_test_17,redis_test_18,redis_test_19,redis_test_20," +
                "redis_test_21,redis_test_22,redis_test_23,redis_test_24,redis_test_25,redis_test_26,redis_test_27,redis_test_28," +
                "redis_test_29,redis_test_30,redis_test_31,redis_test_32,redis_test_33,redis_test_34,redis_test_35,redis_test_36,redis_test_37," +
                "redis_test_38,redis_test_39,redis_test_40,redis_test_41,redis_test_42,redis_test_43,redis_test_44,redis_test_45");

        DataStream<Student5> process = mapStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(processFunction);

        FileSink fileSinkFunction = new FileSink("D:\\code\\github\\Flink-Learning\\flink-stream\\src\\main\\resources\\sink.txt");
        process.addSink(fileSinkFunction);

        //process.printToErr();

        /*
        HbaseSink2 sinkFunction = new HbaseSink2("sink:fink_api_sink_1");
        process.addSink(sinkFunction);*/
        env.execute();


    }
}
