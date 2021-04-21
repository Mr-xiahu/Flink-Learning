package cn.xhjava.flink.stream.async.join.hbase;

import cn.xhjava.domain.Student4;
import cn.xhjava.flink.stream.sink.funcations.HbaseSinkFunction;
import cn.xhjava.flink.stream.source.SourceTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author Xiahu
 * @create 2021/4/20
 * <p>
 * 用Async I/O实现流表与维表Joinpublic
 * 五张表关联查询
 */

class MutilStreamJoin_02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        //1.添加数据源
        SourceFunction<String> kafkaSource = SourceTool.getKafkaSource("flink_kafka_source");
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        //2.转换为POJO流
        DataStream<Student4> mapStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new Student4(fields[0], fields[1], fields[2]);
        });


        //3.流关联
        HbaseAsyncFunction hbaseAsyncFunction1 = new HbaseAsyncFunction("lookup:realtime_dim_1");
        HbaseAsyncFunction hbaseAsyncFunction2 = new HbaseAsyncFunction("lookup:realtime_dim_2");
        HbaseAsyncFunction hbaseAsyncFunction3 = new HbaseAsyncFunction("lookup:realtime_dim_3");
        HbaseAsyncFunction hbaseAsyncFunction4 = new HbaseAsyncFunction("lookup:realtime_dim_4");
        HbaseAsyncFunction hbaseAsyncFunction5 = new HbaseAsyncFunction("lookup:realtime_dim_5");
        HbaseAsyncFunction hbaseAsyncFunction6 = new HbaseAsyncFunction("lookup:realtime_dim_6");
        HbaseAsyncFunction hbaseAsyncFunction7 = new HbaseAsyncFunction("lookup:realtime_dim_7");
        HbaseAsyncFunction hbaseAsyncFunction8 = new HbaseAsyncFunction("lookup:realtime_dim_8");
        HbaseAsyncFunction hbaseAsyncFunction9 = new HbaseAsyncFunction("lookup:realtime_dim_9");
        HbaseAsyncFunction hbaseAsyncFunction10 = new HbaseAsyncFunction("lookup:realtime_dim_10");
        HbaseAsyncFunction hbaseAsyncFunction11 = new HbaseAsyncFunction("lookup:realtime_dim_11");
        HbaseAsyncFunction hbaseAsyncFunction12 = new HbaseAsyncFunction("lookup:realtime_dim_12");
        HbaseAsyncFunction hbaseAsyncFunction13 = new HbaseAsyncFunction("lookup:realtime_dim_13");
        HbaseAsyncFunction hbaseAsyncFunction14 = new HbaseAsyncFunction("lookup:realtime_dim_14");
        HbaseAsyncFunction hbaseAsyncFunction15 = new HbaseAsyncFunction("lookup:realtime_dim_15");
        HbaseAsyncFunction hbaseAsyncFunction16 = new HbaseAsyncFunction("lookup:realtime_dim_16");
        HbaseAsyncFunction hbaseAsyncFunction17 = new HbaseAsyncFunction("lookup:realtime_dim_17");
        HbaseAsyncFunction hbaseAsyncFunction18 = new HbaseAsyncFunction("lookup:realtime_dim_18");
        HbaseAsyncFunction hbaseAsyncFunction19 = new HbaseAsyncFunction("lookup:realtime_dim_19");
        HbaseAsyncFunction hbaseAsyncFunction20 = new HbaseAsyncFunction("lookup:realtime_dim_20");
        HbaseAsyncFunction hbaseAsyncFunction21 = new HbaseAsyncFunction("lookup:realtime_dim_21");
        HbaseAsyncFunction hbaseAsyncFunction22 = new HbaseAsyncFunction("lookup:realtime_dim_22");
        HbaseAsyncFunction hbaseAsyncFunction23 = new HbaseAsyncFunction("lookup:realtime_dim_23");
        HbaseAsyncFunction hbaseAsyncFunction24 = new HbaseAsyncFunction("lookup:realtime_dim_24");
        HbaseAsyncFunction hbaseAsyncFunction25 = new HbaseAsyncFunction("lookup:realtime_dim_25");
        HbaseAsyncFunction hbaseAsyncFunction26 = new HbaseAsyncFunction("lookup:realtime_dim_26");
        HbaseAsyncFunction hbaseAsyncFunction27 = new HbaseAsyncFunction("lookup:realtime_dim_27");
        HbaseAsyncFunction hbaseAsyncFunction28 = new HbaseAsyncFunction("lookup:realtime_dim_28");
        HbaseAsyncFunction hbaseAsyncFunction29 = new HbaseAsyncFunction("lookup:realtime_dim_29");
        HbaseAsyncFunction hbaseAsyncFunction30 = new HbaseAsyncFunction("lookup:realtime_dim_30");
        HbaseAsyncFunction hbaseAsyncFunction31 = new HbaseAsyncFunction("lookup:realtime_dim_31");
        HbaseAsyncFunction hbaseAsyncFunction32 = new HbaseAsyncFunction("lookup:realtime_dim_32");
        HbaseAsyncFunction hbaseAsyncFunction33 = new HbaseAsyncFunction("lookup:realtime_dim_33");
        HbaseAsyncFunction hbaseAsyncFunction34 = new HbaseAsyncFunction("lookup:realtime_dim_34");
        HbaseAsyncFunction hbaseAsyncFunction35 = new HbaseAsyncFunction("lookup:realtime_dim_35");
        HbaseAsyncFunction hbaseAsyncFunction36 = new HbaseAsyncFunction("lookup:realtime_dim_36");
        HbaseAsyncFunction hbaseAsyncFunction37 = new HbaseAsyncFunction("lookup:realtime_dim_37");
        HbaseAsyncFunction hbaseAsyncFunction38 = new HbaseAsyncFunction("lookup:realtime_dim_38");
        HbaseAsyncFunction hbaseAsyncFunction39 = new HbaseAsyncFunction("lookup:realtime_dim_39");
        HbaseAsyncFunction hbaseAsyncFunction40 = new HbaseAsyncFunction("lookup:realtime_dim_40");
        HbaseAsyncFunction hbaseAsyncFunction41 = new HbaseAsyncFunction("lookup:realtime_dim_41");
        HbaseAsyncFunction hbaseAsyncFunction42 = new HbaseAsyncFunction("lookup:realtime_dim_42");
        HbaseAsyncFunction hbaseAsyncFunction43 = new HbaseAsyncFunction("lookup:realtime_dim_43");
        HbaseAsyncFunction hbaseAsyncFunction44 = new HbaseAsyncFunction("lookup:realtime_dim_44");
        HbaseAsyncFunction hbaseAsyncFunction45 = new HbaseAsyncFunction("lookup:realtime_dim_45");

        DataStream<Student4> resultStream1 = AsyncDataStream.orderedWait(mapStream, hbaseAsyncFunction1, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream2 = AsyncDataStream.orderedWait(resultStream1, hbaseAsyncFunction2, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream3 = AsyncDataStream.orderedWait(resultStream2, hbaseAsyncFunction3, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream4 = AsyncDataStream.orderedWait(resultStream3, hbaseAsyncFunction4, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream5 = AsyncDataStream.orderedWait(resultStream4, hbaseAsyncFunction5, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream6 = AsyncDataStream.orderedWait(resultStream5, hbaseAsyncFunction6, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream7 = AsyncDataStream.orderedWait(resultStream6, hbaseAsyncFunction7, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream8 = AsyncDataStream.orderedWait(resultStream7, hbaseAsyncFunction8, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream9 = AsyncDataStream.orderedWait(resultStream8, hbaseAsyncFunction9, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream10 = AsyncDataStream.orderedWait(resultStream9, hbaseAsyncFunction10, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream11 = AsyncDataStream.orderedWait(resultStream10, hbaseAsyncFunction11, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream12 = AsyncDataStream.orderedWait(resultStream11, hbaseAsyncFunction12, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream13 = AsyncDataStream.orderedWait(resultStream12, hbaseAsyncFunction13, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream14 = AsyncDataStream.orderedWait(resultStream13, hbaseAsyncFunction14, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream15 = AsyncDataStream.orderedWait(resultStream14, hbaseAsyncFunction15, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream16 = AsyncDataStream.orderedWait(resultStream15, hbaseAsyncFunction16, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream17 = AsyncDataStream.orderedWait(resultStream16, hbaseAsyncFunction17, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream18 = AsyncDataStream.orderedWait(resultStream17, hbaseAsyncFunction18, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream19 = AsyncDataStream.orderedWait(resultStream18, hbaseAsyncFunction19, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream20 = AsyncDataStream.orderedWait(resultStream19, hbaseAsyncFunction20, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream21 = AsyncDataStream.orderedWait(resultStream20, hbaseAsyncFunction21, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream22 = AsyncDataStream.orderedWait(resultStream21, hbaseAsyncFunction22, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream23 = AsyncDataStream.orderedWait(resultStream22, hbaseAsyncFunction23, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream24 = AsyncDataStream.orderedWait(resultStream23, hbaseAsyncFunction24, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream25 = AsyncDataStream.orderedWait(resultStream24, hbaseAsyncFunction25, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream26 = AsyncDataStream.orderedWait(resultStream25, hbaseAsyncFunction26, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream27 = AsyncDataStream.orderedWait(resultStream26, hbaseAsyncFunction27, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream28 = AsyncDataStream.orderedWait(resultStream27, hbaseAsyncFunction28, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream29 = AsyncDataStream.orderedWait(resultStream28, hbaseAsyncFunction29, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream30 = AsyncDataStream.orderedWait(resultStream29, hbaseAsyncFunction30, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream31 = AsyncDataStream.orderedWait(resultStream30, hbaseAsyncFunction31, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream32 = AsyncDataStream.orderedWait(resultStream31, hbaseAsyncFunction32, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream33 = AsyncDataStream.orderedWait(resultStream32, hbaseAsyncFunction33, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream34 = AsyncDataStream.orderedWait(resultStream33, hbaseAsyncFunction34, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream35 = AsyncDataStream.orderedWait(resultStream34, hbaseAsyncFunction35, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream36 = AsyncDataStream.orderedWait(resultStream35, hbaseAsyncFunction36, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream37 = AsyncDataStream.orderedWait(resultStream36, hbaseAsyncFunction37, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream38 = AsyncDataStream.orderedWait(resultStream37, hbaseAsyncFunction38, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream39 = AsyncDataStream.orderedWait(resultStream38, hbaseAsyncFunction39, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream40 = AsyncDataStream.orderedWait(resultStream39, hbaseAsyncFunction40, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream41 = AsyncDataStream.orderedWait(resultStream40, hbaseAsyncFunction41, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream42 = AsyncDataStream.orderedWait(resultStream41, hbaseAsyncFunction42, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream43 = AsyncDataStream.orderedWait(resultStream42, hbaseAsyncFunction43, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream44 = AsyncDataStream.orderedWait(resultStream43, hbaseAsyncFunction44, 500, TimeUnit.MILLISECONDS, 1000);
        DataStream<Student4> resultStream45 = AsyncDataStream.orderedWait(resultStream44, hbaseAsyncFunction45, 500, TimeUnit.MILLISECONDS, 1000);



        HbaseSinkFunction sinkFunction = new HbaseSinkFunction("sink:fink_api_sink_1");
        resultStream45.addSink(sinkFunction);

        env.execute();


    }
}
