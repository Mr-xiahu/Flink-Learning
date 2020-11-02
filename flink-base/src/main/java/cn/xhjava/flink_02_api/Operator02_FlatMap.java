package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * FlatMap 算子的输入流是 DataStream，
 * 经过 FlatMap 算子后返回的数据格式是 SingleOutputStreamOperator 类型，
 * 获取一个元素并生成零个、一个或多个元素
 */
public class Operator02_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<String> stringDataStream = env.fromElements(DataSource.MAP);
        stringDataStream.flatMap(new FlatMapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {
                for (String word : value.split("\\W+")) {
                    out.collect(new Tuple3<>(word, "China", "ShangHai"));
                }
            }
        }).printToErr();

        //3.执行
        env.execute("flink operator FlatMap");
    }
}
