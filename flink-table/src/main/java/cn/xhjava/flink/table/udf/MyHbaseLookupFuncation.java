//package cn.xhjava.flink.table.udf;
//
//import org.apache.flink.annotation.VisibleForTesting;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
//import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
//import org.apache.flink.connector.hbase.util.HBaseReadWriteHelper;
//import org.apache.flink.connector.hbase.util.HBaseTableSchema;
//import org.apache.flink.table.functions.FunctionContext;
//import org.apache.flink.table.functions.TableFunction;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HConstants;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.TableNotFoundException;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
///**
// * @author Xiahu
// * @create 2021/4/6
// */
//public class MyHbaseLookupFuncation extends TableFunction<String>{
//    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupFunction.class);
//
//    private final String hTableName;
//    private Configuration configuration;
//
//    private transient Connection hConnection;
//    private transient HTable table;
//
//    public MyHbaseLookupFuncation(
//            Configuration configuration, String hTableName, HBaseTableSchema hbaseTableSchema) {
//        this.configuration = configuration;
//        this.hTableName = hTableName;
//    }
//
//    /**
//     * The invoke entry point of lookup function.
//     *
//     * @param rowKey the lookup key. Currently only support single rowkey.
//     */
//    public void eval(String rowKey) throws IOException {
//        // fetch result
//        Result result = table.get(new Get(Bytes.toBytes(rowKey)));
//        if (!result.isEmpty()) {
//            // parse and collect
//            collect(parseColumn(result));
//        }
//    }
//
//
//    public static String parseColumn(Result result) {
//        StringBuffer sb = new StringBuffer();
//
//        //行键
//        String rowkey = Bytes.toString(result.getRow());
//        sb.append(rowkey).append(",");
//        KeyValue[] kvs = result.raw();
//
//        for (KeyValue kv : kvs) {
//            //列族名
//            String family = Bytes.toString(kv.getFamily());
//            //列名
//            String qualifier = Bytes.toString(kv.getQualifier());
//
//            String value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
//            sb.append(qualifier).append(",").append(value).append(",");
//        }
//        return sb.toString();
//    }
//
//
//    @Override
//    public void open(FunctionContext context) {
//        LOG.info("start open ...");
//        try {
//            configuration.set("hbase.zookeeper.quorum", "192.168.0.115");
//            hConnection = ConnectionFactory.createConnection(configuration);
//            table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
//        } catch (TableNotFoundException tnfe) {
//            LOG.error("Table '{}' not found ", hTableName, tnfe);
//            throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
//        } catch (IOException ioe) {
//            LOG.error("Exception while creating connection to HBase.", ioe);
//            throw new RuntimeException("Cannot create connection to HBase.", ioe);
//        }
//        LOG.info("end open.");
//    }
//
//    @Override
//    public void close() {
//        LOG.info("start close ...");
//        if (null != table) {
//            try {
//                table.close();
//                table = null;
//            } catch (IOException e) {
//                // ignore exception when close.
//                LOG.warn("exception when close table", e);
//            }
//        }
//        if (null != hConnection) {
//            try {
//                hConnection.close();
//                hConnection = null;
//            } catch (IOException e) {
//                // ignore exception when close.
//                LOG.warn("exception when close connection", e);
//            }
//        }
//        LOG.info("end close.");
//    }
//
//    @VisibleForTesting
//    public String getHTableName() {
//        return hTableName;
//    }
//}
