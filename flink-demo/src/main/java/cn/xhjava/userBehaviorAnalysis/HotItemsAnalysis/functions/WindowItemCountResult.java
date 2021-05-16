package cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.ItemViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author XiaHu
 * @create 2021/5/16
 */
// 自定义全窗口函数(计算窗口内的所有缓存元素)
public class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
    @Override
    public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        Long itemId = aLong;
        Long windowEnd = window.getEnd();
        Long windwoStart = window.getStart();
        Long count = input.iterator().next();
        out.collect(new ItemViewCount(itemId, windowEnd, windwoStart, count));
    }
}