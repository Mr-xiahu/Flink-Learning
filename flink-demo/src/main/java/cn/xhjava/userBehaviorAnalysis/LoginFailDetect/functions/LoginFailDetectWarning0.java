package cn.xhjava.userBehaviorAnalysis.LoginFailDetect.functions;


import cn.xhjava.userBehaviorAnalysis.domain.LoginEvent;
import cn.xhjava.userBehaviorAnalysis.domain.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author XiaHu
 * @create 2021/5/23
 * LoginFailDetectWarning V-1.0 使用定时器触发报警机制
 * 遇到登录失败的事件时将其保存在 ListState 中，然后设置一个定时器，2 秒后触发。
 * 定时器触发时检查状态中的登录失败事件个数，如果大于等于 2，那么就输出报警信息
 *
 */
public class LoginFailDetectWarning0 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
    // 定义属性，最大连续登录失败次数
    private Integer maxFailTimes;

    public LoginFailDetectWarning0(Integer maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    // 定义状态：保存2秒内所有的登录失败事件
    ListState<LoginEvent> loginFailEventListState;
    // 定义状态：保存注册的定时器时间戳
    ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        // 判断当前登录事件类型
        if ("fail".equals(value.getLoginState())) {
            // 1. 如果是失败事件，添加到列表状态中
            loginFailEventListState.add(value);
            // 如果没有定时器，注册一个2秒之后的定时器
            if (timerTsState.value() == null) {
                Long ts = (value.getTimestamp() + 2) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            }
        } else {
            // 2. 如果是登录成功，删除定时器，清空状态，重新开始
            if (timerTsState.value() != null)
                ctx.timerService().deleteEventTimeTimer(timerTsState.value());
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
        // 定时器触发，说明2秒内没有登录成功来，判断ListState中失败的个数
        ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get());
        Integer failTimes = loginFailEvents.size();

        if (failTimes >= maxFailTimes) {
            // 如果超出设定的最大失败次数，输出报警
            out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                    loginFailEvents.get(0).getTimestamp(),
                    loginFailEvents.get(failTimes - 1).getTimestamp(),
                    "login fail in 2s for " + failTimes + " times"));
        }

        // 清空状态
        loginFailEventListState.clear();
        timerTsState.clear();
    }
}