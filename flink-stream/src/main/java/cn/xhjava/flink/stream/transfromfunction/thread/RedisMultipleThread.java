package cn.xhjava.flink.stream.transfromfunction.thread;

import cn.xhjava.flink.stream.pojo.Student4;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/4/25
 */

@Slf4j
public class RedisMultipleThread implements Runnable {


    private List<Student4> sourceData;
    private Collector<Student4> out;
    private LinkedHashSet<String> tableList;
    private Jedis jedis;
    private String threadName;
    private CountDownLatch countDownLatch;

    public RedisMultipleThread(List<Student4> sourceData, Collector<Student4> out, LinkedHashSet<String> tableList,  CountDownLatch countDownLatch) {
        this.sourceData = sourceData;
        this.out = out;
        this.tableList = tableList;
        this.threadName = Thread.currentThread().getName();
        this.countDownLatch = countDownLatch;
        this.jedis = new Jedis("node2");
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        List<Student4> tmpJoinData = new ArrayList<>();
        LinkedList<String> keyList = new LinkedList<>();
        for (String table : tableList) {
            if (tmpJoinData.isEmpty()) {
                for (int i = 0; i < sourceData.size(); i++) {
                    Student4 student = sourceData.get(i);
                    String key = table + "_" + student.getId();
                    keyList.add(key);
                }

                Map<String, Map<String, String>> redisData = getRedisData(keyList);

                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student4 student : sourceData) {
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
                    Student4 student = tmpJoinData.get(i);
                    String key = table + "_" + student.getId();
                    keyList.add(key);
                }

                Map<String, Map<String, String>> redisData = getRedisData(keyList);


                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student4 student : tmpJoinData) {
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
        long endTime = System.currentTimeMillis();
        long speed = endTime - startTime;
        log.info("{} 线程数据量: {},消耗时间: {} s", threadName, count, (speed / 1000.0));

        sourceData.clear();
        tmpJoinData.clear();
        jedis.close();
        countDownLatch.countDown();
    }


    public Map<String, Map<String, String>> getRedisData(LinkedList<String> keyList) {
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
}
