package cn.xhjava.flink.table.Demo;


import cn.xhjava.domain.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author XiaHu
 * @create 2021/4/19
 * <p>
 * 开窗函数 ScalarFunction
 */
public class Flink_Table_12_Over {
    public static void main(String[] args) {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //2.从文本读取真实数据
        DataStreamSource<String> source = env.readTextFile("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> dataStream = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //3.定义事件时间 Event Time
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

        //4.注册为临时表
        tableEnv.createTemporaryView("sensor", dataTable);

        // 5 Over Window
        // table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id, rt, id.count over ow, temp.avg over ow");

        // SQL
        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");


        tableEnv.toAppendStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");
    }

}