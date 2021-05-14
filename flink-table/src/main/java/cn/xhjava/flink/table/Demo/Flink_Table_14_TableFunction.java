package cn.xhjava.flink.table.Demo;


import cn.xhjava.domain.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author XiaHu
 * @create 2021/4/19
 * <p>
 * TableFunction
 * 用户定义的表函数，也可以将0、1或多个标量值作为输入参数；与标量函数不同的是，
 * 它可以返回任意数量的行作为输出，而不是单个值
 *
 */
public class Flink_Table_14_TableFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\sensor.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3. 将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 4. 自定义表函数，实现将id拆分，并输出（word, length）
        // 4.1 table API
        Split split = new Split("_");

        // 需要在环境中注册UDF
        tableEnv.registerFunction("split", split);
        Table resultTable = sensorTable
                .joinLateral("split(id) as (word, length)")
                .select("id, ts, word, length");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, word, length " +
                " from sensor, lateral table(split(id)) as splitid(word, length)");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义TableFunction
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        // 定义属性，分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        // 必须实现一个eval方法，没有返回值
        public void eval( String str ){
            for( String s: str.split(separator) ){
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }

}