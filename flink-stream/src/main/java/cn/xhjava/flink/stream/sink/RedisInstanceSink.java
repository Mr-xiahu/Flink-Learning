package cn.xhjava.flink.stream.sink;

import cn.xhjava.domain.OggMsg;
import cn.xhjava.redis.RedisClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Xiahu
 * @create 2021/5/11
 */
@Slf4j
public class RedisInstanceSink extends RichSinkFunction<OggMsg> implements CheckpointedFunction {
    private Properties properties;

    private Map<String, String> bufferData = new ConcurrentHashMap<>();
    private ListState<Tuple2<String, String>> checkPointState;

    private LinkedList<String> columnList;
    private LinkedList<Jedis> jedisPools;

    public RedisInstanceSink(Properties properties, LinkedList<String> columnList) {
        this.properties = properties;
        this.columnList = columnList;

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jedisPools = RedisClusterUtils.getJedisPools();
    }

    @Override
    public void invoke(OggMsg value, Context context) throws Exception {
        String line = parseData(value);
        String id = value.getAfter().get(value.getPrimary_keys().get(0));
        synchronized (this) {
            checkPointState.add(new Tuple2<>(value.getTable() + "_" + id, line));
            bufferData.put(value.getTable() + "_" + id, line);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        long startTime = System.currentTimeMillis();
        int count = bufferData.size();
        if (count > 0) {
            synchronized (this) {
                Iterator<Map.Entry<String, String>> iterator = bufferData.entrySet().iterator();
                Map<String, String> dataMap = new HashMap<>();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String key = entry.getKey();
                    String value = bufferData.remove(key);
                    dataMap.put(key, value);
                }
                Map<Jedis, Map<String, String>> pipelineMap = RedisClusterUtils.distributeRedisInstacne(jedisPools, dataMap);
                RedisClusterUtils.setData(pipelineMap);
            }
            long endTime = System.currentTimeMillis();
            log.info("批量写入Redis 多单机节点 数据: {} , 耗费时间: {} s", count, (endTime - startTime) / 1000.0);
        }
        checkPointState.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("running initializeState");
        checkPointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Tuple2<String, String>>("checkpoint-state", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
            @Override
            public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                return super.getTypeInfo();
            }
        })));

    }

    @Override
    public void close() throws Exception {
        super.close();
        RedisClusterUtils.closeJedis();
    }

    private String parseData(OggMsg oggMsg) {
        Map<String, String> after = oggMsg.getAfter();
        if (null == after || after.size() == 0) {
            return null;
        }
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < columnList.size(); i++) {
            String key = columnList.get(i);
            String value = after.get(key);
            if (i == columnList.size() - 1) {
                sb.append(key).append(":").append(value);
            } else {
                sb.append(key).append(":").append(value).append(",");
            }
        }
        return sb.toString();

    }
}
