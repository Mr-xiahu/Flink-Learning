package cn.xhjava.flink_14_coordinator;

import cn.xhjava.flink_13_transform.MyAbstractOperator;
import cn.xhjava.flink_13_transform.MyOperatorFactory;
import cn.xhjava.flink_13_transform.MyProcessFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class CoordinatorDemoOperator<I> extends CoordinatorDemoAbstractOperator<I> {

    public CoordinatorDemoOperator(Configuration conf) {
        super(new CoordinatorDemoProcessFunction(conf));
    }

    public static <I> CoordinatorDemoOperatorFactory<I> getFactory(Configuration conf) {
        return CoordinatorDemoOperatorFactory.instance(conf, new CoordinatorDemoOperator<>(conf));
    }
}
