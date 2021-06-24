package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 实现自定义的增量聚合函数
 */
public class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(MarketingUserBehavior value, Long accumulator) {
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