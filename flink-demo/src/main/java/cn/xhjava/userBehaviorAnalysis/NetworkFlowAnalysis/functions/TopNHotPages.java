package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2021/5/17
 */
// 实现自定义的处理函数
public class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
    private Integer topSize;

    public TopNHotPages(Integer topSize) {
        this.topSize = topSize;
    }

    // 定义状态，保存当前所有PageViewCount到Map中
//        ListState<PageViewCount> pageViewCountListState;
    MapState<String, Long> pageViewCountMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list", PageViewCount.class));
        pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
    }

/*    @Override
    public void processElement(PageViewCount value, KeyedProcessFunction.Context ctx, Collector<String> out) throws Exception {
//            pageViewCountListState.add(value);
        pageViewCountMapState.put(value.getUrl(), value.getCount());
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        // 注册一个1分钟之后的定时器，用来清空状态
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
    }*/

    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
        pageViewCountMapState.put(value.getUrl(), value.getCount());
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        // 注册一个1分钟之后的定时器，用来清空状态
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
        if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
            pageViewCountMapState.clear();
            return;
        }

        ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

        pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                if (o1.getValue() > o2.getValue())
                    return -1;
                else if (o1.getValue() < o2.getValue())
                    return 1;
                else
                    return 0;
            }
        });

        // 格式化成String输出
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("===================================\n");
        resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

        // 遍历列表，取top n输出
        for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
            Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
            resultBuilder.append("NO ").append(i + 1).append(":")
                    .append(" 页面URL = ").append(currentItemViewCount.getKey())
                    .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                    .append("\n");
        }
        resultBuilder.append("===============================\n\n");

        // 控制输出频率
        Thread.sleep(1000L);

        out.collect(resultBuilder.toString());
    }

    /*@Override
    public void onTimer(long timestamp, KeyedProcessFunction.OnTimerContext ctx, Collector<String> out) throws Exception {
        // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
        if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
            pageViewCountMapState.clear();
            return;
        }

        ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

        pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                if (o1.getValue() > o2.getValue())
                    return -1;
                else if (o1.getValue() < o2.getValue())
                    return 1;
                else
                    return 0;
            }
        });

        // 格式化成String输出
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("===================================\n");
        resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

        // 遍历列表，取top n输出
        for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
            Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
            resultBuilder.append("NO ").append(i + 1).append(":")
                    .append(" 页面URL = ").append(currentItemViewCount.getKey())
                    .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                    .append("\n");
        }
        resultBuilder.append("===============================\n\n");

        // 控制输出频率
        Thread.sleep(1000L);

        out.collect(resultBuilder.toString());

//            pageViewCountListState.clear();
    }*/


}
