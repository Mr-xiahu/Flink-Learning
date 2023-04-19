package cn.xhjava.flink_14_coordinator;

import cn.xhjava.flink_13_transform.MyAbstractOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.*;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class CoordinatorDemoOperatorFactory<I>
        extends SimpleUdfStreamOperatorFactory<Object>
        implements CoordinatedOperatorFactory<Object>,OneInputStreamOperatorFactory<I, Object> {
    private final CoordinatorDemoAbstractOperator<I> operator;
    private final Configuration conf;

    public CoordinatorDemoOperatorFactory(Configuration conf, CoordinatorDemoAbstractOperator<I> operator) {
        super(operator);
        this.operator = operator;
        this.conf = conf;
    }

    public static <I,Object> CoordinatorDemoOperatorFactory<I> instance(Configuration conf, CoordinatorDemoAbstractOperator<I> operator) {
        return new CoordinatorDemoOperatorFactory<>(conf, operator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
        final OperatorID operatorID = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

        this.operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorID));
        this.operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        eventDispatcher.registerEventHandler(operatorID, operator);
        return (T) operator;
    }



    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new MyProvider(operatorID,conf);
    }
}
