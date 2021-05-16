package cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author XiaHu
 * @create 2021/5/16
 */
// 实现自定义增量聚合函数
public class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}