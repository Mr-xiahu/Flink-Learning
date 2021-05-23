package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.ChannelPromotionCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author XiaHu
 * @create 2021/5/23
 */
public class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<ChannelPromotionCount> out) throws Exception {
        String windowEnd = new Timestamp(window.getEnd()).toString();
        Long count = input.iterator().next();

        out.collect(new ChannelPromotionCount("total", "total", windowEnd, count));
    }
}