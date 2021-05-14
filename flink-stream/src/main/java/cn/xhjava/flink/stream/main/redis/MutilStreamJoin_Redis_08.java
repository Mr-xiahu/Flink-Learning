package cn.xhjava.flink.stream.main.redis;

import cn.xhjava.flink.stream.pojo.Student4;
import cn.xhjava.flink.stream.sink.HbaseSink;
import cn.xhjava.flink.stream.source.SourceTool;
import cn.xhjava.flink.stream.transfromfunction.CheckpointJoinProcessFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Xiahu
 * @create 2021/4/20
 * <p>
 * map() 内,在进行 checkpoint 时做join redis 集群数据
 */

public class MutilStreamJoin_Redis_08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointTimeout(1200000);
        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://master:8020/tmp/flink/redis");
        env.setStateBackend(fsStateBackend);

        //1.添加数据源
        SourceFunction<String> kafkaSource = SourceTool.getKafkaSource(args[0]);
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        //2.转换为POJO流
        DataStream<Student4> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            Student4 student4 = new Student4(fields[0], fields[1], fields[2]);
            return student4;
        });


        CheckpointJoinProcessFunction processFunction = new CheckpointJoinProcessFunction(
                "xh.testTable_1,xh.testTable_2,xh.testTable_3,xh.testTable_4,xh.testTable_5," +
                        "xh.testTable_6,xh.testTable_7,xh.testTable_8,xh.testTable_9,xh.testTable_10," +
                        "xh.testTable_11,xh.testTable_12,xh.testTable_13,xh.testTable_14,xh.testTable_15," +
                        "xh.testTable_16,xh.testTable_17,xh.testTable_18,xh.testTable_19,xh.testTable_20," +
                        "xh.testTable_21,xh.testTable_22,xh.testTable_23,xh.testTable_24,xh.testTable_25," +
                        "xh.testTable_26,xh.testTable_27,xh.testTable_28,xh.testTable_29,xh.testTable_30," +
                        "xh.testTable_31,xh.testTable_32,xh.testTable_33,xh.testTable_34,xh.testTable_35," +
                        "xh.testTable_36,xh.testTable_37,xh.testTable_38,xh.testTable_39,xh.testTable_40," +
                        "xh.testTable_41,xh.testTable_42,xh.testTable_43,xh.testTable_44,xh.testTable_45");
        //CheckpointJoinProcessFunction processFunction = new CheckpointJoinProcessFunction("redis_test_1,redis_test_2,redis_test_3,redis_test_4,redis_test_5");

        DataStream<Student4> process = mapStream.process(processFunction);

        //process.printToErr();
        HbaseSink sinkFunction = new HbaseSink("sink:fink_api_sink_1");
        process.addSink(sinkFunction);

        env.execute();
    }
}
