package cn.xhjava.flink_06_watermark.watermark;

/**
 * @author Xiahu
 * @create 2020/11/2
 */

import cn.xhjava.domain.Event;
import cn.xhjava.util.DateUtil;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static cn.xhjava.util.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * 周期性添加watermark 推荐使用
 */
public class PeriodicWatermark implements AssignerWithPeriodicWatermarks<Event> {
    private final static Logger log = LoggerFactory.getLogger(PeriodicWatermark.class);

    private long currentTimestamp = Long.MIN_VALUE;


    //解析出事件对象自身携带的时间
    @Override
    public long extractTimestamp(Event event, long previousElementTimestamp) {
        long timestamp = event.getTimeStamp();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        log.info("event timestamp = , {}, CurrentWatermark =  {}",
                DateUtil.format(event.getTimeStamp(), YYYY_MM_DD_HH_MM_SS),
                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return event.getTimeStamp();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }
}
