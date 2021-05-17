package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2021/5/17
 */
// 实现自定义的窗口函数
public class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
    @Override
    public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
        out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
    }
}
