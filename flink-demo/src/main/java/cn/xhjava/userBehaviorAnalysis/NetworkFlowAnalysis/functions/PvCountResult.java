package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2021-05-18
 */
// 实现自定义窗口
public class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
    @Override
    public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
        out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
    }
}
