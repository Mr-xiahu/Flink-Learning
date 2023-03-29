package cn.xhjava.flink_14_coordinator;

import cn.xhjava.flink_14_coordinator.MyOperatorEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
@Slf4j
public class MyOperationCoordinator implements OperatorCoordinator {

    public MyOperationCoordinator(Configuration conf, Context context) {

    }

    @Override
    public void start() throws Exception {
        log.info("coordinator start");
    }

    @Override
    public void close() throws Exception {
        log.info("coordinator close");
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        MyOperatorEvent event1 = (MyOperatorEvent) event;
        log.info("handleEventFromOperator:{} , {}", subtask, event1.getMsg());
    }


    //注意: result一定要有返回值,不然其他算子在进行checkpoint时会卡住

    /**
     * 在CheckpointCoordinator.startTriggeringCheckpoint()方法内,主要是进行checkpoint,包括调用snapshot
     * 在执行snapshot之前,会执行OperatorCoordinatorCheckpoints.triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion
     * 在该方法内部会去调用OperatorCoordinator.checkpointCoordinator()
     * result必须得返回
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception {
        new Thread(() -> {
            try {
                result.complete(new byte[0]);
            } catch (Throwable throwable) {
                // when a checkpoint fails, throws directly.
                result.completeExceptionally(
                        new CompletionException(
                                String.format("Failed to checkpoint Instant %s for source %s",
                                        "Test", this.getClass().getSimpleName()), throwable));
            }
        }).start();

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        log.info("notifyCheckpointComplete");
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {

    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {

    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {

    }


}
