package cn.xhjava.flink.stream.async.join.state;


import cn.xhjava.domain.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

/**
 * @author XiaHu
 * @create 2021/4/7
 */
public class State02_KeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("node2", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print("result");

        env.execute();
    }

    // 自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        // 其它类型状态的声明
        private ListState<String> myListState;
        private String taskName;

        @Override
        public void open(Configuration parameters) throws Exception {

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            taskName = getRuntimeContext().getTaskName();
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            myListState.add(value.getId());

            Iterator<String> iterator = myListState.get().iterator();
            while (iterator.hasNext()) {
                String next = iterator.next();
                System.out.println(taskName + "++++++++++++++++" + new String(next.getBytes()));
            }

            return Integer.valueOf(value.getId());
        }
    }
}