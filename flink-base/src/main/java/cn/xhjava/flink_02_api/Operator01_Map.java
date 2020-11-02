package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * Map算子
 * Map 算子的输入流是 DataStream，
 * 经过 Map 算子后返回的数据格式是 SingleOutputStreamOperator 类型，
 * 获取一个元素并生成一个元素
 * <p>
 * 如果输入的是xiahu   则输出yangming
 */
public class Operator01_Map {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<String> stringDataStream = env.fromElements(DataSource.MAP);
        stringDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split("\\W+")) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                if (s.equals("xiahu")) {
                    return "yangming";
                }
                return s;
            }
        }).printToErr();

        //3.执行
        env.execute("flink operator Map");
    }
}
