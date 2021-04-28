package cn.xhjava.flink.stream.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Xiahu
 * @create 2021/4/26
 */
public class UserDefindWindow_02 extends WindowAssigner<Object, TimeWindow> {

    private Long staggerOffset = null;
    private final WindowStagger windowStagger;
    private final long size;

    private final long globalOffset;


    public UserDefindWindow_02(long size, long offset, WindowStagger windowStagger) {
        if (Math.abs(offset) >= size) {
            throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
        }

        this.size = size;
        this.globalOffset = offset;
        this.windowStagger = windowStagger;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        final long now = context.getCurrentProcessingTime();
        if (staggerOffset == null) {
            staggerOffset = windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
        }
        long start = TimeWindow.getWindowStartWithOffset(now, (globalOffset + staggerOffset) % size, size);
        return Collections.singletonList(new TimeWindow(start, start + size));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new ProcessTimeAndCountTrigger<>();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    public static UserDefindWindow_02 of(Time size) {
        return new UserDefindWindow_02(size.toMilliseconds(), 0, WindowStagger.ALIGNED);
    }
}
