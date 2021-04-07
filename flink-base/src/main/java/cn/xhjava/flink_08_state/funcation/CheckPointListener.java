package cn.xhjava.flink_08_state.funcation;


import org.apache.flink.api.common.state.CheckpointListener;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class CheckPointListener implements CheckpointListener {


    //返回CheckPoint的结束的ID
    /**
     * @param checkpointId The ID of the checkpoint that has been completed.
     * @throws Exception
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        //返回CheckPoint的结束的ID
    }
}
