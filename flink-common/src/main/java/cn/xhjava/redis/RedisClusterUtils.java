package cn.xhjava.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

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

}
