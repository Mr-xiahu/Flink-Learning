package cn.xhjava.flink_13_transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.*;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class MyOperatorFactory<I>
        extends SimpleUdfStreamOperatorFactory<Object>
        implements OneInputStreamOperatorFactory<I, Object> {
    private final MyAbstractOperator<I> operator;
    private final Configuration conf;

    public MyOperatorFactory(Configuration conf, MyAbstractOperator<I> operator) {
        super(operator);
        this.operator = operator;
        this.conf = conf;
    }

    public static <I,Object> MyOperatorFactory<I> instance(Configuration conf, MyAbstractOperator<I> operator) {
        return new MyOperatorFactory<>(conf, operator);
    }
}
