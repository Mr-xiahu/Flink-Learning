package cn.xhjava.flink.stream.transfromfunction;

import cn.xhjava.flink.stream.pojo.Student4;
import cn.xhjava.redis.RedisClusterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/5/10
 */
@Slf4j
public class CheckpointJoinRedisInstanceProcessFunction extends ProcessFunction<Student4, Student4> implements CheckpointedFunction {

    private String hbaseTableName;

    private LinkedHashSet<String> tableList = new LinkedHashSet<>();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private ListState<Student4> checkpointState;
    private Collector<Student4> out;
    private LinkedList<Jedis> jedisPools;

    public CheckpointJoinRedisInstanceProcessFunction(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jedisPools = RedisClusterUtils.getJedisPools();
        String[] fields = hbaseTableName.split(",");
        for (String table : fields) {
            tableList.add(table);
        }
    }

    List<Student4> sourceData;
    List<Student4> tmpJoinData = new ArrayList<>();


    @Override
    public void processElement(Student4 value, Context ctx, Collector<Student4> out) throws Exception {
        this.out = out;
        synchronized (this) {
            checkpointState.add(value);
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (this) {
            Iterator<Student4> iterator = checkpointState.get().iterator();
            sourceData = new ArrayList<>();
            while (iterator.hasNext()) {
                sourceData.add(iterator.next());
                if (sourceData.size() >= 10000) {
                    log.info("sourceData count: {}", sourceData.size());
                    join(sourceData, tmpJoinData);
                    sourceData.clear();
                }
            }
            checkpointState.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Student4>("my-state", Student4.class));
    }


    @Override
    public void close() throws Exception {
        super.close();
    }


    private StringBuffer sb = new StringBuffer();


    private void join(List<Student4> sourceData, List<Student4> tmpJoinData) throws IOException {
        LinkedList<String> keyList = new LinkedList<>();
        long start = System.currentTimeMillis();
        //join查询
        for (String table : tableList) {
            if (tmpJoinData.isEmpty()) {
                for (int i = 0; i < sourceData.size(); i++) {
                    Student4 student = sourceData.get(i);
                    String key = table + "_" + student.getId();
                    keyList.add(key);
                }

                long start1 = System.currentTimeMillis();
                Map<Jedis, LinkedList<String>> map = RedisClusterUtils.getDistributeRedisInstacne(this.jedisPools, keyList);
                Map<String, Map<String, String>> redisData = RedisClusterUtils.getData(map);
                long end1 = System.currentTimeMillis();
                sb.append((end1 - start1) / 1000.0 + " s").append("  ");

                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student4 student : sourceData) {
                    String key = table + "_" + student.getId();
                    if (redisData.containsKey(key)) {
                        Map<String, String> dataMap = redisData.get(key);
                        student.setCity(dataMap.get("table_name"));
                        tmpJoinData.add(student);
                    } else {
                        //如果还是没找到,则表示Hbase内不存在关联数据
                        tmpJoinData.add(student);
                    }
                }
                sourceData.clear();
                redisData.clear();
            } else {
                keyList = new LinkedList<>();

                //判断关联建是否存在于缓存中,如果不存在,去查找
                for (int i = 0; i < tmpJoinData.size(); i++) {
                    Student4 student = tmpJoinData.get(i);
                    String key = table + "_" + student.getId();
                    keyList.add(key);
                }

                long start1 = System.currentTimeMillis();
                Map<Jedis, LinkedList<String>> map = RedisClusterUtils.getDistributeRedisInstacne(this.jedisPools, keyList);
                Map<String, Map<String, String>> redisData = RedisClusterUtils.getData(map);
                long end1 = System.currentTimeMillis();
                sb.append((end1 - start1) / 1000.0 + " s").append("  ");


                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student4 student : tmpJoinData) {
                    String key = table + "_" + student.getId();
                    if (redisData.containsKey(key)) {
                        Map<String, String> dataMap = redisData.get(key);
                        student.setCity(dataMap.get("table_name"));
                        sourceData.add(student);
                    } else {
                        sourceData.add(student);
                    }
                }
                tmpJoinData.clear();
                redisData.clear();
            }
        }

        int count = 0;

        if (tmpJoinData.isEmpty()) {
            for (Student4 student : sourceData) {
                out.collect(student);
            }
            count = sourceData.size();
        } else {
            for (Student4 student : tmpJoinData) {
                out.collect(student);
            }
            count = tmpJoinData.size();
        }
        long end = System.currentTimeMillis();
        long speed = end - start;
        log.info("Current Data Count : {} , Time: {} s ", count, (speed / 1000.0));
        log.info(" Request Redis Time: {}", sb.toString());
        sb = new StringBuffer();
        sourceData.clear();
        tmpJoinData.clear();
    }
}
