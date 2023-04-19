package cn.xhjava.flink_11_processfunction;


import cn.xhjava.domain.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author XiaHu
 * @create 2021/4/8
 */
public class Flink_01_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
//        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> inputStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-base\\src\\main\\resources\\sensor.txt");


        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyKeyedProcess())
                .print();

        env.execute();
    }


    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> implements CheckpointedFunction {

        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取State

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // context
            ctx.timestamp(); // 获取当前数据的时间戳
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
//            ctx.timerService().registerProcessingTimeTimer();
//            ctx.timerService().registerEventTimeTimer();
//            ctx.timerService().deleteEventTimeTimer();
//            ctx.timerService().deleteProcessingTimeTimer();

            // onTimer() 的触发时间
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
//            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        /**
         *  定时器注册，在processElement()内注册，使用ctx.timerService().registerProcessingTimeTimer();
         *  如果定时器注册为：ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
         *  那么在数据进来过5s 后,执行onTimer() 方法
         *
         *  定时器删除:ctx.timerService().deleteProcessingTimeTimer();
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            tsTimerState = context.getKeyedStateStore().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }
    }
}