package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author XiaHu
 * @create 2021/5/23
 */
public class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, Long> value, Long accumulator) {
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