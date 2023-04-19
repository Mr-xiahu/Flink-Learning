package cn.xhjava.flink_14_coordinator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class MyProvider implements OperatorCoordinator.Provider {

    private final OperatorID operatorId;
    private final Configuration conf;

    public MyProvider(OperatorID operatorId, Configuration conf) {
        this.operatorId = operatorId;
        this.conf = conf;
    }


    @Override
    public OperatorID getOperatorId() {
        return operatorId;
    }

    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        return new MyOperationCoordinator(this.conf, context);
    }
}
