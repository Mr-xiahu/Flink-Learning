package cn.xhjava.flink.hbaselookup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
public class Redis_test {
    private Jedis jedis;
    private static String FORMAT = "id:%s,name:%s,age:%s";
    private long startTime;
    private Pipeline pipelined;


    @Before
    public void init() {
        jedis = new Jedis("node2");
        startTime = System.currentTimeMillis();
        pipelined = jedis.pipelined();
    }


    @Test
    public void insertDataToRedis() {
        String tableNmae = "redis_test_5";
        int count = 20000000;
        for (int i = 1; i <= count; i++) {
            pipelined.set(tableNmae + "_" + i, String.format(FORMAT, i, "zhangsan_" + i, "10"));
        }
        pipelined.sync();

    }

    @After
    public void close() {
        jedis.close();
        long endTime = System.currentTimeMillis();
        pipelined.close();
        System.out.println("耗费时间：" + (endTime - startTime) / 1000.0);
    }
}
