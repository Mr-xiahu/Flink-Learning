package cn.xhjava.flink_13_transform;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(10000l);
        env.setStateBackend(new MemoryStateBackend());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        Configuration conf = new Configuration();

        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.113", 8889);

        //dataStream.flatMap(new MyFlatMapRichFunction());

        MyOperatorFactory factory = MyOperator.getFactory(conf);
        dataStream.transform(
                "",
                TypeInformation.of(String.class),
                factory
        );



        env.execute();
    }


    public static String opIdentifier(String operatorN, Configuration conf) {
        return operatorN + ": " + "test";
    }
}
