package cn.xhjava.flink.stream.async.join.redis;

import cn.xhjava.domain.Student4;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/4/20
 */
public class RedisAsyncFunction extends RichAsyncFunction<Student4, Student4> implements CheckpointedFunction {

    private Jedis jedis;
    private String tableName;
    private Cache<String, String> cache;

    public RedisAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        cache = CacheBuilder
                .newBuilder()
                .maximumSize(2000000)
                //.expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
        jedis = new Jedis("node2");
        Set<String> keySet = jedis.keys(tableName + "_*");
        Pipeline pipelined = jedis.pipelined();
        for (String key : keySet) {
            pipelined = jedis.pipelined();
            pipelined.get(key);
        }
        List<Object> values = pipelined.syncAndReturnAll();
        for (Object obj : values) {
            String value = (String) obj;
            //todo
            //cache.put(, value);
        }

    }

    @Override
    public void asyncInvoke(Student4 input, ResultFuture<Student4> resultFuture) throws Exception {

    }

    @Override
    public void timeout(Student4 input, ResultFuture<Student4> resultFuture) throws Exception {

    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }
}
