package cn.xhjava.flink_05_window.Window07_custom_window;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class MyEvictor implements Evictor {
    @Override
    public void evictBefore(Iterable elements, int size, Window window, EvictorContext evictorContext) {

    }

    @Override
    public void evictAfter(Iterable elements, int size, Window window, EvictorContext evictorContext) {

    }
}
