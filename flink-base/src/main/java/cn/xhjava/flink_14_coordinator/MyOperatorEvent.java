package cn.xhjava.flink_14_coordinator;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
public class MyOperatorEvent implements OperatorEvent {
    private String msg;

    public MyOperatorEvent(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
