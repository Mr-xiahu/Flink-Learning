package cn.xhjava.flink.stream.main.test_demo;

import cn.xhjava.domain.OggMsg;
import cn.xhjava.flink.stream.source.SourceTool;
import cn.xhjava.flink.stream.transfromfunction.RowToRowDataMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/5/8
 * <p>
 * 测试: 从kafka内读取数据,封装为Row类型,在从Row 转为RowData
 */
public class KafkaRowToRowData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        Properties properties = new Properties();
        properties.load(new FileReader(new File("D:\\code\\github\\Flink-Learning\\flink-stream\\src\\main\\resources\\application.properties")));


        //1.添加数据源
        SourceFunction<OggMsg> kafkaSource = SourceTool.getOggmsg("flink_kafka_source");
        DataStreamSource<OggMsg> dataStream = env.addSource(kafkaSource);

        SingleOutputStreamOperator<Row> map = dataStream.map(new MapFunction<OggMsg, Row>() {

            @Override
            public Row map(OggMsg value) throws Exception {
                //Row类型封装
                LinkedList<String> list = columnList();
                Row row = new Row(RowKind.INSERT, value.getAfter().size());
                for (int i = 0; i < list.size(); i++) {
                    row.setField(i, value.getAfter().get(list.get(i)));
                }
                return row;
            }
        });

        LinkedList<String> list = columnList();
        String[] array = {"id","fk_id","qfxh","jdpj","nioroa","gwvz","joqtf","isdeleted","lastupdatedttm","rowkey"};
        RowToRowDataMapFunction mapFunction = new RowToRowDataMapFunction(array, null);
        map.map(mapFunction);


        map.printToErr();


        env.execute();
    }


    //模拟获取gp 数据库内column,生产环境下可能从元数据库内获取
    public static LinkedList<String> columnList() {
        LinkedList<String> columnList = new LinkedList<>();
        columnList.addLast("id");
        columnList.addLast("fk_id");
        columnList.addLast("qfxh");
        columnList.addLast("jdpj");
        columnList.addLast("nioroa");
        columnList.addLast("gwvz");
        columnList.addLast("joqtf");
        columnList.addLast("isdeleted");
        columnList.addLast("lastupdatedttm");
        columnList.addLast("rowkey");
        return columnList;
    }
}
