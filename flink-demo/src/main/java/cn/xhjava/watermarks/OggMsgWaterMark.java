package cn.xhjava.watermarks;

import cn.xhjava.domain.OggMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Xiahu
 * @create 2020/11/27
 * 水印规则
 */
@Slf4j
public class OggMsgWaterMark implements AssignerWithPunctuatedWatermarks<OggMsg> {
    private static final SimpleDateFormat DATA_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
    private static final SimpleDateFormat DATA_FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Long currentTimeStamp = 0L;
    //设置允许乱序时间,单位毫秒;
    private Long maxOutOfOrderness = 5000L;


    @Override
    public long extractTimestamp(OggMsg element, long previousElementTimestamp) {
        String current_ts = element.getCurrent_ts();
        Date event_time = null;
        try {
            event_time = DATA_FORMAT.parse(current_ts);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        currentTimeStamp = event_time.getTime();
        log.info("Event_Time: {} , Watermark: {}", DATA_FORMAT2.format(currentTimeStamp), DATA_FORMAT2.format(currentTimeStamp - maxOutOfOrderness));
        return currentTimeStamp;
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(OggMsg lastElement, long extractedTimestamp) {
        return new Watermark(currentTimeStamp - maxOutOfOrderness);
    }

    /**
     *  当时间时间(currentTimeStamp) > 水印时间时,下游可以收到全部的数据,可能会产生延迟
     *  当时间时间(currentTimeStamp) = 水印时间时,下游可以收到全部的数据,不会产生延迟
     *  当时间时间(currentTimeStamp) < 水印时间时,下游不会收到数据
     */
}
