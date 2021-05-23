package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * @author Xiahu
 * @create 2021-05-18
 * 布隆过滤器本质上是一种数据结构，比较巧妙的概率型数据结构（probabilistic data structure），
 * 特点是高效地插入和查询，可以用来告诉你 “某样东西一定不存在或者可能存在”。
 * 它本身是一个很长的二进制向量，既然是二进制的向量，那么显而易见的，存放的不是 0，就是 1。
 * 相比于传统的 List、Set、Map 等数据结构，它更高效、占用空间更少，但是缺点是其返回的结果是概率性的，而不是确切的
 */
// 实现自定义的处理函数
public class UvCountResultWithBloomFliter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
    // 定义jedis连接和布隆过滤器
    Jedis jedis;
    MyBloomFilter myBloomFilter;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("localhost", 6379);
        myBloomFilter = new MyBloomFilter(1 << 29);    // 要处理1亿个数据，用64MB大小的位图
    }

    @Override
    public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
        // 将位图和窗口count值全部存入redis，用windowEnd作为key
        Long windowEnd = context.window().getEnd();
        String bitmapKey = windowEnd.toString();
        // 把count值存成一张hash表
        String countHashName = "uv_count";
        String countKey = windowEnd.toString();

        // 1. 取当前的userId
        Long userId = elements.iterator().next().getUserId();

        // 2. 计算位图中的offset
        Long offset = myBloomFilter.hashCode(userId.toString(), 61);

        // 3. 用redis的getbit命令，判断对应位置的值
        Boolean isExist = jedis.getbit(bitmapKey, offset);

        if (!isExist) {
            // 如果不存在，对应位图位置置1
            jedis.setbit(bitmapKey, offset, true);

            // 更新redis中保存的count值
            Long uvCount = 0L;    // 初始count值
            String uvCountString = jedis.hget(countHashName, countKey);
            if (uvCountString != null && !"".equals(uvCountString))
                uvCount = Long.valueOf(uvCountString);
            jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));

            out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
        }
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
