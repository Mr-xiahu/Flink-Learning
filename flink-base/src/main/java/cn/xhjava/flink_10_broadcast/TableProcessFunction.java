package cn.xhjava.flink_10_broadcast;

import cn.xhjava.flink_10_broadcast.other.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2022/2/16 0016
 *
 * 使用广播流,将元数据流与实时数据流进行关联
 */
@Slf4j
public class TableProcessFunction extends BroadcastProcessFunction<String, String, String> {
    private List<OutputTag<String>> outputTagList;
    private Map<String, OutputTag<String>> outputMap;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private List<TableProcess> tableProcessList;
    private Map<String, TableProcess> tableProcessMap;

    public TableProcessFunction(List<OutputTag<String>> outputTagList, MapStateDescriptor<String, TableProcess> mapStateDescriptor, List<TableProcess> tableProcessList) {
        this.outputTagList = outputTagList;
        this.outputMap = new HashMap<>();
        this.tableProcessMap = new HashMap<>();
        this.mapStateDescriptor = mapStateDescriptor;
        this.tableProcessList = tableProcessList;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (tableProcessList.size() > 0) {
            for (TableProcess tableProcess : tableProcessList) {
                createOutPutTag(tableProcess);
            }
        }

    }

    /***
     * value： 正常OGG格式数据
     */
    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        JSONObject json = JSON.parseObject(value, Feature.OrderedField);

        String key = String.format("%s.%s", "db", "table");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            splitOutPut(tableProcess, json, ctx);
        } else {
            if (this.tableProcessMap.containsKey(key)) {
                tableProcess = this.tableProcessMap.get(key);
                splitOutPut(tableProcess, json, ctx);
            } else {
                log.warn("该组合Key：" + key + "不存在！");
            }
        }
    }

    private void splitOutPut(TableProcess tableProcess, JSONObject json, ReadOnlyContext ctx) {
        String fullName = String.format("%s.%s", tableProcess.getDbName(), tableProcess.getTableName());
        //分流到不同的topic
        if (outputMap.containsKey(fullName)) {
            json.put("sink_topic", tableProcess.getSinkTopic());
            OutputTag<String> outputTag = outputMap.get(fullName);
            ctx.output(outputTag, JSONObject.toJSONString(json, SerializerFeature.WriteMapNullValue, SerializerFeature.MapSortField));
        }
    }


    /**
     * value:
     * {
     * "database": "flink_realtime",
     * "before": {
     * "mode": "spark",
     * "db_name": "xh_1",
     * "id": 1,
     * "table_name": "test_1",
     * "sink_topic": "topic_A",
     * "sink_param": ""
     * },
     * "after": {
     * "mode": "spark",
     * "db_name": "xh_2",
     * "id": 1,
     * "table_name": "test_1",
     * "sink_topic": "topic_A",
     * "sink_param": ""
     * },
     * "type": "update",
     * "tableName": "t_table_process"
     * }
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        createOutPutTag(tableProcess);

        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = String.format("%s.%s", tableProcess.getDbName(), tableProcess.getTableName());
        broadcastState.put(key, tableProcess);
    }


    private void createOutPutTag(TableProcess tableProcess) {
        //2.创建侧输出流
        String fullName = String.format("%s.%s", tableProcess.getDbName(), tableProcess.getTableName());
        if (!outputMap.containsKey(fullName)) {
            OutputTag<String> tableTag = new OutputTag<String>(String.format("%s-tag", fullName)) {
            };
            outputMap.put(fullName, tableTag);
            this.tableProcessMap.put(fullName, tableProcess);
            outputTagList.add(tableTag);
        } else {
            OutputTag<String> tableTag = new OutputTag<String>(String.format("%s-tag", fullName)) {
            };
            OutputTag<String> outputTag = outputMap.get(fullName);
            outputTagList.remove(outputTag);
            outputMap.put(fullName, tableTag);
            this.tableProcessMap.put(fullName, tableProcess);
            outputTagList.add(tableTag);
        }
    }
}
