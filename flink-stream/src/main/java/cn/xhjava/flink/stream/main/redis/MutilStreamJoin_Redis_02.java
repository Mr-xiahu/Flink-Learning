package cn.xhjava.flink.stream.main.redis;

import cn.xhjava.flink.stream.pojo.Student4;
import cn.xhjava.flink.stream.sink.HbaseSink;
import cn.xhjava.flink.stream.source.SourceTool;
import cn.xhjava.flink.stream.transfromfunction.MyRedisProcessAllWindowFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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
 * TumblingProcessingTimeWindows + function + redis + keyby
 */

class MutilStreamJoin_Redis_02 {
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
        DataStream<Student4> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            Student4 student4 = new Student4(fields[0], fields[1], fields[2]);
            if (Integer.valueOf(student4.getId()) % 2 == 0) {
                student4.setId("1");
            } else {
                student4.setId("2");
            }
            return student4;
        });

        MyRedisProcessAllWindowFunction processFunction = new MyRedisProcessAllWindowFunction("redis_test_1,redis_test_2,redis_test_3,redis_test_4,redis_test_5");

        DataStream<Student4> process = mapStream
                .keyBy(new KeySelector<Student4, Object>() {
                    @Override
                    public Object getKey(Student4 value) throws Exception {
                        return value.getId();
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(processFunction);


        //process.printToErr();

        HbaseSink sinkFunction = new HbaseSink("sink:fink_api_sink_1");
        process.addSink(sinkFunction);
        env.execute();


    }
}
