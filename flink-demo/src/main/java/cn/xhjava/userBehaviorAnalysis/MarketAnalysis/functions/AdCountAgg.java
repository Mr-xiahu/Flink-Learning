package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions;


import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.AdClickEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 广告总数量聚集函数
 */
public class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent value, Long accumulator) {
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