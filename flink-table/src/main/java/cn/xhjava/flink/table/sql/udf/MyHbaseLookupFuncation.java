package cn.xhjava.flink.table.sql.udf;

import cn.xhjava.domain.HbaseResult;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author Xiahu
 * @create 2021/4/7
 *
 *  自定义实现HbaseLookupFuncation,返回值---String
 * TableFunction<String>
 */
public class MyHbaseLookupFuncation extends TableFunction<String> {
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
        } else {
            collect(parseColumn(null));
        }
    }


    public String parseColumn(Result result) {
        HbaseResult hbaseResult = new HbaseResult();
        hbaseResult.setTableName(hTableName);

        if (null != result) {
            hbaseResult.setRowkey(Bytes.toString(result.getRow()));
            HashMap<String, String> column = new HashMap<>();
            KeyValue[] kvs = result.raw();
            for (KeyValue kv : kvs) {
                String family = Bytes.toString(kv.getFamily());
                String qualifier = Bytes.toString(kv.getQualifier());
                String value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
                column.put(family + ":" + qualifier, value);
            }
            hbaseResult.setColumn(column);
        } else {
            hbaseResult.setRowkey(null);
            hbaseResult.setColumn(null);
        }
        String jsonString = JSON.toJSONString(hbaseResult, SerializerFeature.MapSortField, SerializerFeature.WriteMapNullValue);

        return jsonString;
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
