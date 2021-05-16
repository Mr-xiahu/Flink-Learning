package cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author XiaHu
 * @create 2021/5/16
 */

// 实现自定义KeyedProcessFunction
public class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
    // 定义属性，top n的大小
    private Integer topSize;

    public TopNHotItems(Integer topSize) {
        this.topSize = topSize;
    }

    // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
    ListState<ItemViewCount> itemViewCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 每来一条数据，存入List中，并注册定时器
        itemViewCountListState.add(value);
        //窗口结束后1s,触发定时器执行相关业务逻辑
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发，当前已收集到所有数据，排序输出
        ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

        itemViewCounts.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return o2.getCount().intValue() - o1.getCount().intValue();
            }
        });

        // 将排名信息格式化成String，方便打印输出
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("===================================\n");
//        resultBuilder.append("窗口开始时间：").append(new Timestamp(timestamp - 1)).append("\n");
        resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

        // 遍历列表，取top n输出
        for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
            ItemViewCount currentItemViewCount = itemViewCounts.get(i);
            resultBuilder.append("NO ").append(i + 1).append(":")
                    .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                    .append(" 热门度 = ").append(currentItemViewCount.getCount())
                    .append("\n");
        }
        resultBuilder.append("===============================\n\n");

        // 控制输出频率
        Thread.sleep(1000L);

        out.collect(resultBuilder.toString());
    }
}
