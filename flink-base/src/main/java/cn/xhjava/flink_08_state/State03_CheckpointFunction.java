package cn.xhjava.flink_08_state;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author Xiahu
 * @create 2020/11/2
 *
 * 实现CheckpointedFunction接口
 */
public class State03_CheckpointFunction implements CheckpointedFunction {

    //执行checkpoint时调用此方法
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    //flink程序初始化时被调用,或者是从State恢复
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        /**
         * initializeState() 方法会在每次初始化用户定义的函数时,或者从更早的 checkpoint 恢复的时候被调用.
         * 因此 initializeState() 不仅是不同类型的状态被初始化的地方,而且还是 state 恢复逻辑的地方.
         */
    }
}
