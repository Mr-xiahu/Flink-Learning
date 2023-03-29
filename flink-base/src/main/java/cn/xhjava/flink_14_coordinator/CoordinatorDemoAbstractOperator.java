package cn.xhjava.flink_14_coordinator;

import cn.xhjava.flink_13_transform.MyAbstractProcessFunction;
import cn.xhjava.flink_13_transform.MyProcessFunction;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ProcessOperator;

/**
 * @author Xiahu
 * @create 2023/3/28 0028
 */
public class CoordinatorDemoAbstractOperator<I> extends ProcessOperator<I, Object> implements OperatorEventHandler,BoundedOneInput {

    private final CoordinatorDemoAbstractProcessFunction function;



    public CoordinatorDemoAbstractOperator(CoordinatorDemoAbstractProcessFunction function) {
        super(function);
        this.function = function;
    }


    @Override
    public void endInput() {
        this.function.endInput();
    }

    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        this.function.setOperatorEventGateway(operatorEventGateway);
    }


    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        this.function.handleOperatorEvent(evt);
    }
}
