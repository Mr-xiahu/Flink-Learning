package cn.xhjava.userBehaviorAnalysis.LoginFailDetect.functions;


import cn.xhjava.userBehaviorAnalysis.domain.LoginEvent;
import cn.xhjava.userBehaviorAnalysis.domain.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 登陆失败检测警告处理函数
 */
public class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
    // 定义属性，最大连续登录失败次数
    private Integer maxFailTimes;

    public LoginFailDetectWarning(Integer maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    // 定义状态：保存2秒内所有的登录失败事件
    ListState<LoginEvent> loginFailEventListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
    }


    // 以登录事件作为判断报警的触发条件，不再注册定时器
    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        // 判断当前事件登录状态
        if( "fail".equals(value.getLoginState()) ){
            // 1. 如果是登录失败，获取状态中之前的登录失败事件，继续判断是否已有失败事件
            Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
            if( iterator.hasNext() ){
                // 1.1 如果已经有登录失败事件，继续判断时间戳是否在2秒之内
                // 获取已有的登录失败事件
                LoginEvent firstFailEvent = iterator.next();
                if( value.getTimestamp() - firstFailEvent.getTimestamp() <= 2 ){
                    // 1.1.1 如果在2秒之内，输出报警
                    out.collect( new LoginFailWarning(value.getUserId(), firstFailEvent.getTimestamp(), value.getTimestamp(), "login fail 2 times in 2s") );
                }

                // 不管报不报警，这次都已处理完毕，直接更新状态
                loginFailEventListState.clear();
                loginFailEventListState.add(value);
            } else {
                // 1.2 如果没有登录失败，直接将当前事件存入ListState
                loginFailEventListState.add(value);
            }
        } else {
            // 2. 如果是登录成功，直接清空状态
            loginFailEventListState.clear();
        }

    }
}