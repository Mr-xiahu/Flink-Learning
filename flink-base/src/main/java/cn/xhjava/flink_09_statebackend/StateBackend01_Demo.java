package cn.xhjava.flink_09_statebackend;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class StateBackend01_Demo {
    public static void main(String[] args) throws IOException {
        /**
         * StateBackend 一共有三种方式如下:
         */

        //MemoryStateBackend:基于内存的State存储
        //FsStateBackend:基于文件系统的State存储 hdfs
        //RocksDBStateBackend:基于rocksdb的文件存储(生产环境使用)

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(60000);//60秒做一次checkpoint

        //设置checkpoint的方式
        StateBackend stateBackend = new MemoryStateBackend();
        StateBackend fsStateBackend = new FsStateBackend("/user/flink/checkpoint");
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("/user/flink/checkpoint", true);

        /*env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // CheckPoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间，只允许 有 1 个 Checkpoint 在发生
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 两次 Checkpoint 之间的最小时间间隔为 500 毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 当 Flink 任务取消时，保留外部保存的 CheckPoint 信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 作业最多允许 Checkpoint 失败 1 次（flink 1.9 开始支持）
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        // Checkpoint 失败后，整个 Flink 任务也会失败（flink 1.9 之前）
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);*/

        env.setStateBackend(stateBackend);
    }

    public void method() {
        //1.用于存储和进行状态 checkpoint 的状态后端存储方式，无默认值
        CheckpointingOptions.STATE_BACKEND.defaultValue();
        //2.要保留的已完成 checkpoint 的最大数量，默认值为 1
        CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
        //3. 状态后端是否使用异步快照方法，默认值为 true
        CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue();
        //4.状态后端是否创建增量检查点，默认值为 false
        CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();
        //5.状态后端配置本地恢复，默认情况下，本地恢复被禁用
        CheckpointingOptions.LOCAL_RECOVERY.defaultValue();
        //6.定义存储本地恢复的基于文件的状态的目录
        CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS.defaultValue();
        //7.存储 savepoints 的目录
        CheckpointingOptions.SAVEPOINT_DIRECTORY.defaultValue();
        //8.存储 checkpoint 的数据文件和元数据
        CheckpointingOptions.CHECKPOINTS_DIRECTORY.defaultValue();
        //9.状态数据文件的最小大小，默认值是 1024
        CheckpointingOptions.FS_SMALL_FILE_THRESHOLD.defaultValue();
    }
}
