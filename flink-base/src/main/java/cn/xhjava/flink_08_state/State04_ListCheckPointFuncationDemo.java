package cn.xhjava.flink_08_state;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * @author Xiahu
 * @create 2020/11/2
 *
 * ListCheckpointed
 */
public class State04_ListCheckPointFuncationDemo extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {

    //当前偏移量
    private Long offset = 0L;

    //作业取消标志
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) {
        final Object lock = ctx.getCheckpointLock();
        /**
         * 为了保证状态的更新和结果的输出原子性,用户必须在 source 的 context 上加锁.
         */
        while (isRunning) {
            //输出和状态更新是原子性的
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }


    /**
     *
     * @param checkpointId          单调递增的长整型数据
     * @param checkpointTimestamp   Checkpoint发生的实际时间
     * @return
     */
    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) {
        //将当前的offset保存到State
        return Collections.singletonList(offset);
    }


    //从State中取出offset的值
    @Override
    public void restoreState(List<Long> state) {
        for (Long s : state)
            offset = s;
    }
}
