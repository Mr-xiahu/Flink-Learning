package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.AdClickEvent;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.BlackListUserWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 过滤黑名单用户函数
 */
public class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {
    // 定义属性：点击次数上限
    private Integer countUpperBound;

    public FilterBlackListUser(Integer countUpperBound) {
        this.countUpperBound = countUpperBound;
    }

    // 定义状态，保存当前用户对某一广告的点击次数
    ValueState<Long> countState;
    // 定义一个标志状态，保存当前用户是否已经被发送到了黑名单里
    ValueState<Boolean> isSentState;

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class, 0L));
        isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
    }

    @Override
    public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
        // 判断当前用户对同一广告的点击次数，如果不够上限，就count加1正常输出；如果达到上限，直接过滤掉，并侧输出流输出黑名单报警
        // 首先获取当前的count值
        Long curCount = countState.value();

        // 1. 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
        if (curCount == 0) {
            Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
//                System.out.println(new Timestamp(ts));
            ctx.timerService().registerProcessingTimeTimer(ts);
        }

        // 2. 判断是否报警
        if (curCount >= countUpperBound) {
            // 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
            if (!isSentState.value()) {
                isSentState.update(true);    // 更新状态
                ctx.output(
                        new OutputTag<BlackListUserWarning>("blacklist") {},
                        new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + countUpperBound + "times."));
            }
            return;    // 不再执行下面操作
        }

        // 如果没有返回，点击次数加1，更新状态，正常输出当前数据到主流
        countState.update(curCount + 1);
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
        // 清空所有状态
        countState.clear();
        isSentState.clear();
    }
}