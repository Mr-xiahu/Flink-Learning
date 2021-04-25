package cn.xhjava.flink.stream.async.join.redis;

import cn.xhjava.flink.stream.pojo.Student4;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021/4/21
 */
@Slf4j
public class MyRedisProcessAllWindowFunctionMultipleThread extends ProcessAllWindowFunction<Student4, Student4, TimeWindow> implements CheckpointedFunction {
    private Jedis jedis;
    private String hbaseTableName;
    private static int THREAD_COUNT = 6;
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private LinkedHashSet<String> tableList = new LinkedHashSet<>();

//    private Map<String, Map<String, String>> cache = new HashMap<>();


    public MyRedisProcessAllWindowFunctionMultipleThread(String hbaseTableName) {
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


    @Override
    public void process(Context context, Iterable<Student4> elements, Collector<Student4> out) throws Exception {

        long startTime = System.currentTimeMillis();

        List<Student4> sourceData = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);

        //1.遍历迭代器,获取关联键
        Iterator<Student4> iterator = elements.iterator();
        while (iterator.hasNext()) {
            sourceData.add(iterator.next());
        }

        log.info("开始批量处理: {}  count: {}", sdf.format(new Date()), sourceData.size());

        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
        int batchSize = sourceData.size() / THREAD_COUNT;

        int finalCount = sourceData.size();


        List<Student4> tmpSourceData = new ArrayList<>();
        for (int i = 0; i < sourceData.size(); i++) {
            tmpSourceData.add(sourceData.get(i));
            if (tmpSourceData.size() >= batchSize) {
                threadPool.execute(new RedisMultipleThread(tmpSourceData, out, tableList, countDownLatch));
                tmpSourceData = new ArrayList<>();
            }
        }


        try {
            //等待，等待全部线程执行完毕才执行
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
        long endTime = System.currentTimeMillis();
        long speed = endTime - startTime;
        log.info("当前批次总数据量: {},消耗时间: {} s", finalCount, (speed / 1000.0));
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
