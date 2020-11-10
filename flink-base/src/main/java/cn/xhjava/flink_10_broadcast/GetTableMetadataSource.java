package cn.xhjava.flink_10_broadcast;

import cn.xhjava.constant.NuwaConstant;
import cn.xhjava.util.MysqlUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import scala.xml.MetaData;

import java.util.List;

/**
 * @author Xiahu
 * @create 2020/11/9
 * <p>
 * 定时读取mysql数据库中的元数据信息
 */
public class GetTableMetadataSource extends RichSourceFunction<List<MetaData>> {
    private MysqlUtil mysql;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        mysql = new MysqlUtil(parameterTool.getProperties(), NuwaConstant.NUWA_UTIL_FLAG);
    }


    @Override
    public void run(SourceContext<List<MetaData>> ctx) throws Exception {
//        List<MetaData> metaData = NuwaConsumerHelper.getMetaData(mysql);
        //循环从mysql读取数据
        List<MetaData> metaData = null;
        ctx.collect(metaData);
    }

    @Override
    public void cancel() {
        mysql.close();
    }
}
