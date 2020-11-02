package cn.xhjava.flink_05_window.Window07_custom_window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class MyWindowAssigner extends WindowAssigner {
    @Override
    public Collection assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        return null;
    }

    @Override
    public Trigger getDefaultTrigger(StreamExecutionEnvironment env) {
        return null;
    }

    @Override
    public TypeSerializer getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
