import cn.xhjava.redis.RedisClusterUtils;
import cn.xhjava.redis.JedisClusterPipeline;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.List;

/**
 * @author Xiahu
 * @create 2021/5/11
 */
public class JedisClusterPiplineTest {
    public static void main(String[] args) throws IOException {
        JedisCluster jedisCluster = RedisClusterUtils.getJedisCluster();

        long s = System.currentTimeMillis();

        JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(jedisCluster);
        jcp.refreshCluster();
        List<Object> batchResult = null;
        try {
            // batch write
            for (int i = 0; i < 10000000; i++) {
                jcp.set("k" + i, "v1" + i);
            }
            jcp.sync();

            // batch read
//            for (int i = 0; i < 10000; i++) {
//                jcp.get("k" + i);
//            }
            batchResult = jcp.syncAndReturnAll();
        } finally {
            jcp.close();
            jedisCluster.close();
        }

        // output time
        long t = System.currentTimeMillis() - s;
        System.out.println(t / 1000.0);


    }
}
