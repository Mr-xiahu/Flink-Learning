package cn.xhjava.util;

import cn.xhjava.constant.NuwaConstant;
import org.apache.flink.api.java.utils.ParameterTool;
import cn.xhjava.constant.FlinkLearnConstant.*;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.Serializable;

import static cn.xhjava.constant.FlinkLearnConstant.*;

/**
 * @author Xiahu
 * @create 2020/11/26
 * flink 集群环境配置类
 */
public class FlinkEnvConfig implements Serializable {
    //    private static String checkPointPath = null;
    private static String checkPointType = null;
    private static StateBackend stateBackend = null;
//    private static String checkPointTime = null;


    //flink集群配置
    public static void buildEnv(ParameterTool parameter, StreamExecutionEnvironment env) {
        env.getConfig().enableForceAvro();
        //checkpoint配置
        checkPointType = parameter.get(FLINK_CHECKPOINT_TYPE);
        if (checkPointType.equals(FILE_SYSTEM)) {
            stateBackend = new FsStateBackend(parameter.get(FLINK_CHECKPOINT_PATH));
        } else if (checkPointType.equals(ROCKS_DB)) {
            try {
                stateBackend = new RocksDBStateBackend(parameter.get(FLINK_CHECKPOINT_PATH), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            stateBackend = new MemoryStateBackend();
        }
        env.setStateBackend(stateBackend);
        env.enableCheckpointing(Integer.valueOf(parameter.get(FLINK_CHECKPOINT_TIME)));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.setParallelism(Integer.valueOf(parameter.get(FLINK_ALL_PARALLEELISM)));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }
}
