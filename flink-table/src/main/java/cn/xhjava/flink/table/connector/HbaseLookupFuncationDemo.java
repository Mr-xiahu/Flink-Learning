package cn.xhjava.flink.table.connector;

import cn.xhjava.domain.Student2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * @author Xiahu
 * @create 2021/4/6
 */
public class HbaseLookupFuncationDemo {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
//        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.0.113", 8888);
        DataStream<Student2> map = sourceStream.map(new MapFunction<String, Student2>() {
            @Override
            public Student2 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student2(Integer.valueOf(fields[0]), fields[1], fields[2]);
            }
        });

        Table student = tableEnv.fromDataStream(map, "id,name,sex");
        tableEnv.createTemporaryView("student", student);


        MyHbaseLookupFuncation baseLookupFunction = new MyHbaseLookupFuncation("test:hbase_user_behavior");

        //注册函数
        tableEnv.registerFunction("hbaseLookup", baseLookupFunction);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select id,name,sex,info from student,LATERAL TABLE(hbaseLookup(id)) as T(info)");


        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();


    }

    public static class MyHbaseLookupFuncation extends TableFunction<String> {
        private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupFunction.class);

        private final String hTableName;
        private Configuration configuration;

        private transient Connection hConnection;
        private transient HTable table;

        public MyHbaseLookupFuncation(String hTableName) {
            this.hTableName = hTableName;
        }

        /**
         * @param rowKey the lookup key. Currently only support single rowkey.
         */
        public void eval(Object rowKey) throws IOException {
            // fetch result
            Result result = table.get(new Get(Bytes.toBytes(String.valueOf(rowKey))));
            if (!result.isEmpty()) {
                // parse and collect
                collect(parseColumn(result));
            }
        }


        public String parseColumn(Result result) {
            StringBuffer sb = new StringBuffer();

            //行键
            String rowkey = Bytes.toString(result.getRow());
            sb.append(rowkey).append(",");
            KeyValue[] kvs = result.raw();

            for (KeyValue kv : kvs) {
                //列族名
                String family = Bytes.toString(kv.getFamily());
                //列名
                String qualifier = Bytes.toString(kv.getQualifier());

                String value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
                sb.append(qualifier).append(",").append(value).append(",");
            }
            return sb.toString();
        }


        @Override
        public void open(FunctionContext context) {
            System.out.println("建立连接！！！！");
            try {
                Configuration configuration = new Configuration();
                configuration.set("hbase.zookeeper.quorum", "192.168.0.115");
                hConnection = ConnectionFactory.createConnection(configuration);
                table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
            } catch (TableNotFoundException tnfe) {
                throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
            } catch (IOException ioe) {
                LOG.error("Exception while creating connection to HBase.", ioe);
            }
            LOG.info("end open.");
        }

        @Override
        public void close() {
            LOG.info("start close ...");
            if (null != table) {
                try {
                    table.close();
                    table = null;
                } catch (IOException e) {
                    // ignore exception when close.
                    LOG.warn("exception when close table", e);
                }
            }
            if (null != hConnection) {
                try {
                    hConnection.close();
                    hConnection = null;
                } catch (IOException e) {
                    // ignore exception when close.
                    LOG.warn("exception when close connection", e);
                }
            }
            LOG.info("end close.");
        }
    }
}
