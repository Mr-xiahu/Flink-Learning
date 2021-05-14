package cn.xhjava.flink.table.sql.udf;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseReadWriteHelper;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/7
 * 自定义实现HbaseLookupFuncation,批量查询,结果有问题
 */
public class MyHbaseLookupFuncationBatch extends TableFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String hTableName;
    private final byte[] serializedConfig;
    private final HBaseTableSchema hbaseTableSchema;

    private transient HBaseReadWriteHelper readHelper;
    private transient Connection hConnection;
    private transient HTable table;

    private LinkedList<String> rowkKeyList;

    private static long THRESHOLD = 10;

    public MyHbaseLookupFuncationBatch(
            Configuration configuration, String hTableName, HBaseTableSchema hbaseTableSchema) {
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(configuration);
        this.hTableName = hTableName;
        this.hbaseTableSchema = hbaseTableSchema;
    }

    LinkedList<Get> getList = null;

    /**
     * The invoke entry point of lookup function.
     *
     * @param rowKey the lookup key. Currently only support single rowkey.
     */
    public void eval(Object rowKey) throws IOException {
        try {
            rowkKeyList.add((String) rowKey);
            if (rowkKeyList.size() >= THRESHOLD) {
                getList = new LinkedList<>();
                for (String rk : rowkKeyList) {
                    getList.add(new Get(Bytes.toBytes(rk)));
                }
                rowkKeyList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (getList != null && getList.size() >= THRESHOLD) {
            Result[] results = table.get(getList);
            for (Result result : results) {
                if (!result.isEmpty()) {
                    collect(readHelper.parseToRow(result, Bytes.toString(result.getRow())));
                }
            }
            getList.clear();
        } else {
            collect(null);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return hbaseTableSchema.convertsToTableSchema().toRowType();
    }

    private org.apache.hadoop.conf.Configuration prepareRuntimeConfiguration() {
        org.apache.hadoop.conf.Configuration runtimeConfig =
                HBaseConfigurationUtil.deserializeConfiguration(
                        serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());
        if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
            LOG.error(
                    "can not connect to HBase without {} configuration",
                    HConstants.ZOOKEEPER_QUORUM);
            throw new IllegalArgumentException(
                    "check HBase configuration failed, lost: '"
                            + HConstants.ZOOKEEPER_QUORUM
                            + "'!");
        }
        return runtimeConfig;
    }

    @Override
    public void open(FunctionContext context) {
        rowkKeyList = new LinkedList<>();
        LOG.info("start open ...");
        org.apache.hadoop.conf.Configuration config = prepareRuntimeConfiguration();
        try {
            hConnection = ConnectionFactory.createConnection(config);
            table = (HTable) hConnection.getTable(TableName.valueOf(hTableName));
        } catch (TableNotFoundException tnfe) {
            LOG.error("Table '{}' not found ", hTableName, tnfe);
            throw new RuntimeException("HBase table '" + hTableName + "' not found.", tnfe);
        } catch (IOException ioe) {
            LOG.error("Exception while creating connection to HBase.", ioe);
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }
        this.readHelper = new HBaseReadWriteHelper(hbaseTableSchema);
        LOG.info("end open.");
    }


    @Override
    public void close() {
        rowkKeyList.clear();
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


    @VisibleForTesting
    public String getHTableName() {
        return hTableName;
    }
}
