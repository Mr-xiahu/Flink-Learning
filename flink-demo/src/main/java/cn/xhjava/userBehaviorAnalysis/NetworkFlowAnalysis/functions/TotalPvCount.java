package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2021-05-18
 */
// 实现自定义处理函数，把相同窗口分组统计的count值叠加
public class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
    // 定义状态，保存当前的总count值
    ValueState<Long> totalCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
    }


    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
        totalCountState.update(totalCountState.value() + value.getCount());
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
        // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
        Long totalCount = totalCountState.value();
        out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
        // 清空状态
        totalCountState.clear();
    }
}
