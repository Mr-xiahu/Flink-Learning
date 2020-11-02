package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.Student;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 * KeyBy 在逻辑上是基于 key 对流进行分区，
 * 相同的 Key 会被分到一个分区（这里分区指的就是下游算子多个并行节点的其中一个）。
 * 在内部，它使用 hash 函数对流进行分区。它返回 KeyedDataStream 数据流。
 *
 *
 * keyBy 将Key相同的进行分组计算...
 */
public class Operator04_KeyBy {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(DataSource.Studens);
        DataStreamSource<Tuple4<Integer, String, String, Integer>> tuple4DataStreamSource = env.fromElements(DataSource.Tuple4_Student);

        //1.按照对象内的属性值分组
        /*studentDataStreamSource.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return value.getSex();
            }
        }).printToErr();*/

        //2.按照对象内的属性值分组,并且指定属性类型
        /*studentDataStreamSource.keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student value) throws Exception {
                return value.getSorce();
            }
        }, Types.INT).printToErr();*/

        //3.只能是元组的数据类型才能使用
        /*tuple4DataStreamSource.keyBy(3).printToErr();*/

        //4.指定对象内的任意属性分组
        /*studentDataStreamSource.keyBy("sex").printToErr();*/

        //3.执行
        env.execute("flink operator KeyBy");
    }
}
