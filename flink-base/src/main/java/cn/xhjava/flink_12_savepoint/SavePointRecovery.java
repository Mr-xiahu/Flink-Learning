package cn.xhjava.flink_12_savepoint;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author Xiahu
 * @create 2021/12/24 0024
 * 测试任务在kill后是否会从checkpoint 点恢复数据
 */
@Slf4j
public class SavePointRecovery {
    public static void main(String[] args) throws Exception {
        String checkPointBasePath = "hdfs://192.168.0.111:8020/tmp/SavePointRecovery-checkpoint";
        Tuple2<String, String> lastChecpointInfo = CheckpointUtil.getLastChecpointInfo(checkPointBasePath);

        Configuration configuration = new Configuration();
        if (lastChecpointInfo == null) {
            log.info("{} :not has chk directory", checkPointBasePath);
        } else {
            log.info("local checkpoint recover, path: {} time:{}", lastChecpointInfo.f0, lastChecpointInfo.f1);
            configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, lastChecpointInfo.f1);
        }
        //configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, "hdfs://192.168.0.111:8020/tmp/SavePointRecovery-checkpoint/9da2f38946dc067794955026af704822/chk-3/_metadata");
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(configuration);
        env.enableCheckpointing(20000);
        env.setParallelism(1);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new RocksDBStateBackend(checkPointBasePath, true));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // the host and the port to connect to
        final String hostname = "192.168.0.113";
        final int port = 8889;


        DataStream<String> text = env.socketTextStream(hostname, port);
        text.flatMap(new MyFlatMapRichFunction());
        env.execute("Socket Window WordCount");
    }
}

