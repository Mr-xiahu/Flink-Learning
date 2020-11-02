package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.Student;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * ataStream API 支持各种聚合，例如 min、max、sum 等。
 * 这些函数可以应用于 KeyedStream 以获得 Aggregations 聚合
 */
public class Operator06_Aggregation {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(DataSource.Studens);
        DataStreamSource<Tuple4<Integer, String, String, Integer>> tuple4DataStreamSource = env.fromElements(DataSource.Tuple4_Student);

        /**
         * KeyedStream.sum(0)
         * KeyedStream.sum("key")
         * KeyedStream.min(0)
         * KeyedStream.min("key")
         * KeyedStream.max(0)
         * KeyedStream.max("key")
         * KeyedStream.minBy(0)
         * KeyedStream.minBy("key")
         * KeyedStream.maxBy(0)
         * KeyedStream.maxBy("key")
         */

        /*studentDataStreamSource.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return value.getSex();
            }
        }).sum("score").printToErr();*/

        tuple4DataStreamSource.keyBy(2).max(3).printToErr();


        //3.执行
        env.execute("flink operator Aggregation");
    }
}
