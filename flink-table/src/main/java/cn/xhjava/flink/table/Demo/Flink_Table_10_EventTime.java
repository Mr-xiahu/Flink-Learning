package cn.xhjava.flink.table.Demo;

import cn.xhjava.domain.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/1
 * <p>
 * 时间特性（Time Attributes） ----- Table 内添加Event Time
 */
public class Flink_Table_10_EventTime {
    public static void main(String[] args) throws Exception {
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

        //3.1 由 DataStream 转换成表时指定
        Table dataStr = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");

        //3.2 定义 Table Schema 时指定,不推荐使用，有点问题
        tableEnv
                .connect(new FileSystem().path("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        /*.rowtime(new Rowtime()
                                .timestampsFromField("timestamp")    // 从字段中提取时间戳
                                .watermarksPeriodicBounded(1000)    // watermark延迟1秒
                        )*/
                        .field("temperature", DataTypes.DOUBLE())
                )

                .createTemporaryTable("intputTable");

        Table tableApi = tableEnv.from("intputTable");

        //3.3 创建表的 DDL 中定义
        String sinkDDL=
                "create table dataTable (" +
                        " id varchar(20) not null, " +
                        " ts bigint, " +
                        " temperature double, " +
                        " rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ), " +
                        " watermark for rt as rt - interval '1' second" +
                        ") with (" +
                        " 'connector.type' = 'filesystem', " +
                        " 'connector.path' = 'F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\sensor.txt', " +
                        " 'format.type' = 'csv')";



        tableEnv.sqlUpdate(sinkDDL);
        Table sql = tableEnv.sqlQuery("select * from dataTable");


        tableEnv.toAppendStream(dataStr, Row.class).printToErr("dataStr");
        tableEnv.toAppendStream(tableApi, Row.class).printToErr("tableApi");
        tableEnv.toAppendStream(sql, Row.class).printToErr("sql");

        env.execute();

    }
}
