package cn.xhjava.flink_13_transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
@Slf4j
public abstract class MyAbstractProcessFunction<I> extends ProcessFunction<I, Object> implements BoundedOneInput {

    // 当被通知,后面不再有数据到来时被调用(批处理即将结束时)
    @Override
    public void endInput() {
        System.out.println("结束了!!!");
    }
}
