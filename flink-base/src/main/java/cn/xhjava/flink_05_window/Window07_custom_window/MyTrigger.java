package cn.xhjava.flink_05_window.Window07_custom_window;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class MyTrigger extends Trigger {
    @Override
    public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {

    }
}
