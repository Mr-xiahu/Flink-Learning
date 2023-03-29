package cn.xhjava.flink_13_transform;

import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ProcessOperator;

/**
 * @author Xiahu
 * @create 2023/3/28 0028
 */
public class MyAbstractOperator<I> extends ProcessOperator<I, Object> implements BoundedOneInput {

    private final MyAbstractProcessFunction function;



    public MyAbstractOperator(MyProcessFunction function) {
        super(function);
        this.function = function;
    }


    @Override
    public void endInput() {
        this.function.endInput();
    }


}
