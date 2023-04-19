package cn.xhjava.flink_13_transform;

import org.apache.flink.configuration.Configuration;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class MyOperator<I> extends MyAbstractOperator<I> {

    public MyOperator(Configuration conf) {
        super(new MyProcessFunction(conf));
    }

    public static <I> MyOperatorFactory<I> getFactory(Configuration conf) {
        return MyOperatorFactory.instance(conf, new MyOperator<>(conf));
    }
}
