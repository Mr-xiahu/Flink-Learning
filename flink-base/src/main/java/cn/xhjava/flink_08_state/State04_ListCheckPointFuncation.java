package cn.xhjava.flink_08_state;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.List;

/**
 * @author Xiahu
 * @create 2020/11/2
 *
 * ListCheckpointed
 */
public class State04_ListCheckPointFuncation implements ListCheckpointed {


    /**
     * 获取函数的当前状态。状态必须返回此函数先前所有的调用结果。
     */
    @Override
    public List snapshotState(long l, long l1) throws Exception {
        return null;
    }

    /**
     * 将函数或算子的状态恢复到先前 checkpoint 的状态.
     * 此方法在故障恢复后执行函数时调用.
     * 如果函数的特定并行实例无法恢复到任何状态，则状态列表可能为空.
     */
    @Override
    public void restoreState(List list) throws Exception {

    }
}
