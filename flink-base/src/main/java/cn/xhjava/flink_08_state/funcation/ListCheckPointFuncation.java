package cn.xhjava.flink_08_state.funcation;

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
public class ListCheckPointFuncation implements ListCheckpointed {


    //执行checkpoint时调用此方法,将state序列化到磁盘(保存到hdfs)
    @Override
    public List snapshotState(long l, long l1) throws Exception {
        return null;
    }

    //当程序发生异常时,从list内恢复状态
    @Override
    public void restoreState(List list) throws Exception {

    }
}
