package cn.xhjava.userBehaviorAnalysis.MarketAnalysis.util;


import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 实现自定义的模拟市场用户行为数据源
 */
public class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {
    // 控制是否正常运行的标识位
    Boolean running = true;

    // 定义用户行为和渠道的范围
    List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
    List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

    Random random = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while(running){
            // 随机生成所有字段
            Long id = random.nextLong();
            String behavior = behaviorList.get( random.nextInt(behaviorList.size()) );
            String channel = channelList.get( random.nextInt(channelList.size()) );
            Long timestamp = System.currentTimeMillis();

            // 发出数据
            ctx.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));

            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}