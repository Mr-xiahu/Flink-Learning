package cn.xhjava.flink.table.udf.tablefuncation;

import cn.xhjava.domain.HbaseResult;
import cn.xhjava.domain.HbaseUserBehavior;
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
 * 自定义实现HbaseLookupFuncation,返回值---POJO
 * TableFunction<HbaseUserBehavior>
 */
public class MyHbaseLookupFuncation3 extends TableFunction<HbaseUserBehavior> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupFunction.class);

    private final String hTableName;
    private Configuration configuration;

    private transient Connection hConnection;
    private transient HTable table;

    public MyHbaseLookupFuncation3(String hTableName) {
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


    public HbaseUserBehavior parseColumn(Result result) {
        HbaseUserBehavior resulta = null;
        if (null != result) {
            resulta = new HbaseUserBehavior();
            resulta.setRowkey(Bytes.toString(result.getRow()));

            HashMap<String, String> column = new HashMap<>();
            KeyValue[] kvs = result.raw();
            for (KeyValue kv : kvs) {
                String family = Bytes.toString(kv.getFamily());
                String qualifier = Bytes.toString(kv.getQualifier());
                String value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
                column.put(family + ":" + qualifier, value);
                if (qualifier.equals("man")) {
                    resulta.setMan(value);
                } else if (qualifier.equals("small")) {
                    resulta.setSmall(value);
                } else {
                    resulta.setYellow(value);
                }
            }

        }

        return resulta;
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
