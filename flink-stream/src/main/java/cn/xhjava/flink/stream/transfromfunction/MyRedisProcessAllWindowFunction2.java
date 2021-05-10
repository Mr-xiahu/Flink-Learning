package cn.xhjava.flink.stream.transfromfunction;

import cn.xhjava.domain.Student5;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/21
 *
 * 处理时间 + 单线程 + 时间戳(keyBy() 后处理)
 */
@Slf4j
public class MyRedisProcessAllWindowFunction2 extends ProcessAllWindowFunction<Student5, Student5, TimeWindow> implements CheckpointedFunction {
    private Jedis jedis;
    private String hbaseTableName;

    private LinkedHashSet<String> tableList = new LinkedHashSet<>();


    public MyRedisProcessAllWindowFunction2(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String[] fields = hbaseTableName.split(",");
        for (String table : fields) {
            tableList.add(table);
        }
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    @Override
    public void process(Context context, Iterable<Student5> elements, Collector<Student5> out) throws Exception {

        List<Student5> sourceData = new ArrayList<>();
        List<Student5> tmpJoinData = new ArrayList<>();

        //1.遍历迭代器,获取关联键
        Iterator<Student5> iterator = elements.iterator();
        while (iterator.hasNext()) {
            sourceData.add(iterator.next());
        }

        log.info("开始批量处理: {}  count: {}", sdf.format(new Date()), sourceData.size());

        LinkedList<String> keyList = new LinkedList<>();
        join(sourceData, tmpJoinData, keyList,out);

    }


    public Map<String, Map<String, String>> getRedisData(LinkedList<String> keyList) throws IOException {
        Map<String, Map<String, String>> cacheMap = new HashMap<>();
        Pipeline pipeline = jedis.pipelined();
        for (String key : keyList) {
            pipeline.get(key);
        }
        List<Object> objects = pipeline.syncAndReturnAll();
        for (Object value : objects) {
            if (null != value) {
                String line = (String) value;
                //id:1,name:zhangsan_redis_test_5,age:redis_test_5
                String[] fields = line.split(",");
                Map<String, String> map = new HashMap<>();

                for (String str : fields) {
                    String[] split = str.split(":");
                    map.put("info:" + split[0], split[1]);
                }
                cacheMap.put(keyList.removeFirst(), map);
            }
        }
        pipeline.close();

        return cacheMap;
    }

    private void join(List<Student5> sourceData, List<Student5> tmpJoinData, LinkedList<String> keyList,Collector<Student5> out) throws IOException {
        long start = System.currentTimeMillis();
        //join查询
        for (String table : tableList) {
            if (tmpJoinData.isEmpty()) {
                for (int i = 0; i < sourceData.size(); i++) {
                    Student5 student = sourceData.get(i);
                    String key = table + "_" + student.getId();
                    keyList.add(key);
                }

                Map<String, Map<String, String>> redisData = getRedisData(keyList);

                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student5 student : sourceData) {
                    String key = table + "_" + student.getId();
                    if (redisData.containsKey(key)) {
                        Map<String, String> dataMap = redisData.get(key);
                        student.setCity(dataMap.get("info:name"));
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
                    Student5 student = tmpJoinData.get(i);
                    String key = table + "_" + student.getId();
                    keyList.add(key);
                }

                Map<String, Map<String, String>> redisData = getRedisData(keyList);


                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student5 student : tmpJoinData) {
                    String key = table + "_" + student.getId();
                    if (redisData.containsKey(key)) {
                        Map<String, String> dataMap = redisData.get(key);
                        student.setCity(dataMap.get("info:name"));
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
            for (Student5 student : sourceData) {
                out.collect(student);
            }
            count = sourceData.size();
        } else {
            for (Student5 student : tmpJoinData) {
                out.collect(student);
            }
            count = tmpJoinData.size();
        }
        long end = System.currentTimeMillis();
        long speed = end - start;
        log.info("当前批次数据量为: {},消耗时间: {} s", count, (speed / 1000.0));

        sourceData.clear();
        tmpJoinData.clear();
    }


    @Override
    public void close() throws Exception {
        super.close();
        tableList.clear();
        jedis.close();


    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        jedis = new Jedis("node2");
    }
}
