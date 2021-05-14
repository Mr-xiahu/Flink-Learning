package cn.xhjava.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/28
 */
public class RedisClusterUtils {
    private final static Logger log = LoggerFactory.getLogger(RedisClusterUtils.class);


    public static JedisCluster getJedisCluster() {
        Set<HostAndPort> hostAndPortSet = new HashSet<HostAndPort>();
        hostAndPortSet.add(new HostAndPort("192.168.0.101", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.102", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.103", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.104", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.105", 6458));

        JedisCluster jedisCluster = new JedisCluster(hostAndPortSet, RedisClusterUtils.getGenericObjectPoolConfig());
        return jedisCluster;
    }

    private static GenericObjectPoolConfig getGenericObjectPoolConfig() {
        GenericObjectPoolConfig genericObjectPool = new GenericObjectPoolConfig();
        genericObjectPool.setMaxIdle(10);
        genericObjectPool.setMaxTotal(100);
        genericObjectPool.setMinEvictableIdleTimeMillis(30000);
        genericObjectPool.setSoftMinEvictableIdleTimeMillis(60000);
        return genericObjectPool;
    }

    private static LinkedList<Jedis> jedisList;

    public static LinkedList<Jedis> getJedisPools() {
        jedisList = new LinkedList<>();
        jedisList.add(new Jedis("192.168.0.105", 7000));
        jedisList.add(new Jedis("192.168.0.105", 7001));
        jedisList.add(new Jedis("192.168.0.105", 7002));
        jedisList.add(new Jedis("192.168.0.105", 7003));
        jedisList.add(new Jedis("192.168.0.105", 7004));
        jedisList.add(new Jedis("192.168.0.105", 7005));
        jedisList.add(new Jedis("192.168.0.105", 7006));
        jedisList.add(new Jedis("192.168.0.105", 7007));
        return jedisList;
    }

    public static void closeJedis() {
        for (Jedis jedis : jedisList) {
            jedis.close();
        }
    }

    //分发数据,根据key的哈希值,将数据均匀分配到节点
    public static Map<Jedis, Map<String, String>> distributeRedisInstacne(LinkedList<Jedis> jedisPools, Map<String, String> data) {
        Map<Jedis, Map<String, String>> result = new HashMap<>();
        Iterator<Map.Entry<String, String>> entryIterator = data.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            Jedis jedis = jedisPools.get(Math.abs(entry.getKey().hashCode()) % jedisPools.size());
            if (result.containsKey(jedis)) {
                result.get(jedis).put(entry.getKey(), entry.getValue());
            } else {
                Map<String, String> kV = new HashMap<>();
                kV.put(entry.getKey(), entry.getValue());
                result.put(jedis, kV);
            }
        }
        return result;
    }


    //分发key,根据key 的哈希 去不同的节点查询数据
    public static Map<Jedis, LinkedList<String>> getDistributeRedisInstacne(LinkedList<Jedis> jedisPools, LinkedList<String> data) {
        Map<Jedis, LinkedList<String>> result = new HashMap<>();
        for (String key : data) {
            Jedis jedis = jedisPools.get(Math.abs(key.hashCode()) % jedisPools.size());
            if (result.containsKey(jedis)) {
                result.get(jedis).add(key);
            } else {
                LinkedList<String> list = new LinkedList<>();
                list.add(key);
                result.put(jedis, list);
            }
        }
        return result;
    }


    /**
     * set() 操作
     *
     * @param data redis 管道与 K-V 的映射,表示哪些数据 需要在写入具体的节点
     */
    public static void setData(Map<Jedis, Map<String, String>> data) throws IOException {
        Iterator<Map.Entry<Jedis, Map<String, String>>> entryIterator = data.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Jedis, Map<String, String>> entry = entryIterator.next();
            Pipeline pipelined = entry.getKey().pipelined();
            Iterator<Map.Entry<String, String>> iterator = entry.getValue().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                pipelined.set(next.getKey(), next.getValue());
            }
            pipelined.sync();
            pipelined.close();
        }
    }

    /**
     * get() 操作
     *
     * @param data redis 管道与key list的映射,表示哪些key 需要在该pipeline才能查询
     * @return xh.testTable_1_1     id:1
     * name:a
     * age:12
     */
    public static Map<String, Map<String, String>> getData(Map<Jedis, LinkedList<String>> data ) throws IOException {
        Map<String, Map<String, String>> cacheMap = new HashMap<>();
        Iterator<Map.Entry<Jedis, LinkedList<String>>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Jedis, LinkedList<String>> entry = iterator.next();
            Pipeline pipeline = entry.getKey().pipelined();
            LinkedList<String> keyList = entry.getValue();
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
                        map.put(split[0], split[1]);
                    }
                    String key = keyList.removeFirst();
                    map.put("table_name", key);
                    cacheMap.put(key, map);
                }
            }
            pipeline.close();
        }

        return cacheMap;
    }


    //获取redis 与 管道的映射,避免每次都要初始化管道,从而浪费开销
//    public static LinkedHashMap<Jedis, Pipeline> buildJedisPiplineMap(LinkedList<Jedis> jedisList) {
//        LinkedHashMap<Jedis, Pipeline> result = new LinkedHashMap<>();
//        for (Jedis jedis : jedisList) {
//            result.put(jedis, jedis.pipelined());
//        }
//        return result;
//    }

}
