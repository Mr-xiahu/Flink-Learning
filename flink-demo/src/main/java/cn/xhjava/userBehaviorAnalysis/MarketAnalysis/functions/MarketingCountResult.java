package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.ChannelPromotionCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 实现自定义的全窗口函数
 */
public class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {//<In,Out,Key,Window>

    @Override
    public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
        //key
        String channel = tuple.getField(0);
        String behavior = tuple.getField(1);
        String windowEnd = new Timestamp(context.window().getEnd()).toString();
        Long count = elements.iterator().next();

        out.collect(new ChannelPromotionCount(channel, behavior, windowEnd, count));
    }
}