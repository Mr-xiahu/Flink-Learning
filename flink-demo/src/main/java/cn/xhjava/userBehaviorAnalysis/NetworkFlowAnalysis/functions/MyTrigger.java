package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Xiahu
 * @create 2021-05-18
 */
// 自定义触发器
public class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
    @Override
    public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // 每一条数据来到，直接触发窗口计算，并且直接清空窗口
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
    }
}
