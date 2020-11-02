package cn.xhjava.flink_05_window.Window07_custom_window;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class MyWindowedStream<T, K, W extends Window> {
    private final KeyedStream<T, K> input;

    private final WindowAssigner<? super T, W> windowAssigner;

    private Trigger<? super T, ? super W> trigger;

    private Evictor<? super T, ? super W> evictor;


    public MyWindowedStream(KeyedStream<T, K> input,
                            WindowAssigner<? super T, W> windowAssigner,
                            Trigger<? super T, ? super W> trigger,
                            Evictor<? super T, ? super W> evictor) {
        this.input = input;
        this.windowAssigner = windowAssigner;
        this.trigger = trigger;
        this.evictor = evictor;
    }
}
