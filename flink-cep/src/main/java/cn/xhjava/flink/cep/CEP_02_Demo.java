package cn.xhjava.flink.cep;


import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author XiaHu
 * @create 2021/11/10
 * <p>
 * CEP 编程三部曲
 * 如何使用CEO
 */
public class CEP_02_Demo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);


        //TODO CEP 三部曲

        //TODO 1.开发模式序列
        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value.equals("a")) {
                            return true;
                        }
                        return false;
                    }
                })
                .next("next")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value.equals("b")) {
                            return true;
                        }
                        return false;
                    }
                }).within(Time.seconds(10));


        //TODO 2.将parrern应用到数据流上
        PatternStream<String> patternStream = CEP.pattern(stream, pattern);

        //TODO 3.从patternStream中提取匹配的数据
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag,
                (PatternTimeoutFunction<String, String>) (pattern1, timeoutTimestamp) -> pattern1.get("start").get(0),
                (PatternSelectFunction<String, String>) pattern12 -> pattern12.get("start").get(0));

        DataStream<String> timeOutStream = selectDS.getSideOutput(timeOutTag);


        //输出
        selectDS.print("select>>>>");
        timeOutStream.print("timeout>>>>>");
    }
}