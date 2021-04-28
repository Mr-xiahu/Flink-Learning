package cn.xhjava.flink.stream.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author Xiahu
 * @create 2021/4/26
 */
public class ProcessTimeAndCountTrigger<W extends Window> extends Trigger<Object, W> {
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new ProcessTimeAndCountTrigger.Sum(), LongSerializer.INSTANCE);
    private final long maxCount = 10;

    public ProcessTimeAndCountTrigger(){}

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
        count.add(1L);
        if (count.get() >= maxCount) {
            count.clear();
            return TriggerResult.FIRE;
        }else{
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }

    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }

    }
}
