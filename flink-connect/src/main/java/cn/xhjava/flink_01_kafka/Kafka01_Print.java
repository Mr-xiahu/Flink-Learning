package cn.xhjava.flink_01_kafka;

import cn.xhjava.flink_01_kafka.hander.MyFlinkKafkaConsumer;
import cn.xhjava.flink_01_kafka.kafka.KafkaConsumer;
import cn.xhjava.util.ParameterToolUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2020/11/4
 */
public class Kafka01_Print {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        KafkaConsumer kafkaConsumer = new KafkaConsumer(parameterTool);
        MyFlinkKafkaConsumer<String> sourceKafka = kafkaConsumer.buildFlinkKafkaConsumer(parameterTool);
        sourceKafka.setStartFromEarliest();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> source = env.addSource(sourceKafka);


        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject object = JSON.parseObject(value, Feature.OrderedField);
                String currentTs = object.getString("current_ts");
                if (currentTs.contains("2020-10-29")) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        //打印执行计划
        System.out.println(env.getExecutionPlan());

        env.execute();

    }
}
