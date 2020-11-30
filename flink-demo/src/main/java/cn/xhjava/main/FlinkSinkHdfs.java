package cn.xhjava.main;

import cn.xhjava.domain.OggMsg;
import cn.xhjava.util.FlinkEnvConfig;
import cn.xhjava.flink_01_kafka.hander.MyFlinkKafkaConsumer;
import cn.xhjava.flink_01_kafka.kafka.KafkaConsumer;
import cn.xhjava.udf.functions.KafkaConsumeCallBack;
import cn.xhjava.udf.functions.process.OggMsgProcessAllWindowFuncation;
import cn.xhjava.util.ParameterToolUtil;
import cn.xhjava.watermarks.OggMsgWaterMark;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Xiahu
 * @create 2020/11/26s
 * flink 消费 kafka , 落地到 hdfs
 */
@Slf4j
public class FlinkSinkHdfs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //加载配置
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        FlinkEnvConfig.buildEnv(parameterTool, env);

        //获取flink kafka source
        KafkaConsumer kafkaConsumer = new KafkaConsumer(parameterTool);
        MyFlinkKafkaConsumer<OggMsg> consumerOggMsg = kafkaConsumer.buildFlinkKafkaConsumerOggMsg(new KafkaConsumeCallBack());
        DataStreamSource<OggMsg> oggMsgStream = env.addSource(consumerOggMsg);
        OggMsgWaterMark oggMsgWaterMark = new OggMsgWaterMark();
        OggMsgProcessAllWindowFuncation funcation = new OggMsgProcessAllWindowFuncation();
        //OggMsgProcessWindowFuncation funcation = new OggMsgProcessWindowFuncation();
        oggMsgStream.
                assignTimestampsAndWatermarks(oggMsgWaterMark)
                .keyBy(new KeySelector<OggMsg, Object>() {
                    @Override
                    public Object getKey(OggMsg value) throws Exception {
                        return value.getTable();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(funcation)//统计处理数据量
                .printToErr();
        //尚未实现
        env.execute("");
    }
}
