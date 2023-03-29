package cn.xhjava.flink_14_coordinator;

import cn.xhjava.flink_12_savepoint.MyFlatMapRichFunction;
import cn.xhjava.flink_13_transform.MyOperator;
import cn.xhjava.flink_13_transform.MyOperatorFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;

/**
 * @author Xiahu
 * @create 2023/3/28 0028
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
        CoordinatorDemoOperatorFactory factory = CoordinatorDemoOperator.getFactory(conf);
        dataStream.transform(
                "",
                TypeInformation.of(String.class),
                factory
        );

        env.execute();
    }
}
