package cn.xhjava.flink_06_watermark.watermark;

import cn.xhjava.domain.Event;
import cn.xhjava.domain.Word;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author Xiahu
 * @create 2020/11/2
 *
 * 连续性的添加watermark
 */
public class PunctuatedWatermark implements AssignerWithPunctuatedWatermarks<Event> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Event lastElement, long extractedTimestamp) {
        return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(Event element, long previousElementTimestamp) {
        return element.getTimeStamp();
    }
}
