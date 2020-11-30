package cn.xhjava.udf.functions.process;

import cn.xhjava.domain.OggMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author Xiahu
 * @create 2020/11/27
 */
@Slf4j
public class OggMsgProcessWindowFuncation extends ProcessWindowFunction<OggMsg, OggMsg, Object, GlobalWindow> {

    private int currentBatchCount;

    public OggMsgProcessWindowFuncation() {

    }

    @Override
    public void process(Object key, Context context, Iterable<OggMsg> elements, Collector<OggMsg> out) throws Exception {
        Iterator<OggMsg> oggMsgIterator = elements.iterator();
        while (oggMsgIterator.hasNext()) {
            OggMsg oggMsg = oggMsgIterator.next();
            out.collect(oggMsg);
            currentBatchCount++;
        }
        log.info("当前处理总数据量处理数据 {} 条", currentBatchCount);
    }
}
