package deserialize;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterType;
import pojo.DebeziumBean;
import pojo.OggMsg;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Xiahu
 * @create 2022/6/16 0016
 */
public class JsonToOggSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private transient JsonConverter jsonConverter;
    private final Boolean includeSchema;
    private Map<String, Object> customConverterConfigs;

    public JsonToOggSchema() {
        this(false);
    }

    public JsonToOggSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    public JsonToOggSchema(Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs = customConverterConfigs;
    }

    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        if (this.jsonConverter == null) {
            this.initializeJsonConverter();
        }

        byte[] bytes = this.jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String key = String.valueOf(record.key()).replace("Struct{","").replace("}","");
        DebeziumBean debeziumBean = JSON.parseObject(new String(bytes),DebeziumBean.class);
        String str = converStr(debeziumBean, key);
        out.collect(str);
    }

    private void initializeJsonConverter() {
        this.jsonConverter = new JsonConverter();
        HashMap<String, Object> configs = new HashMap(2);
        configs.put("converter.type", ConverterType.VALUE.getName());
        configs.put("schemas.enable", this.includeSchema);
        if (this.customConverterConfigs != null) {
            configs.putAll(this.customConverterConfigs);
        }

        this.jsonConverter.configure(configs);
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public static String converStr(DebeziumBean debeziumBean, String key) throws JsonProcessingException {
        OggMsg oggMsg = new OggMsg();
        Map<Object, Object> before = debeziumBean.getBefore();
        Map<Object, Object> after = debeziumBean.getAfter();
        oggMsg.setBefore(before);
        oggMsg.setAfter(after);
        List<String> primary = new ArrayList<>();
        List<String> list = Arrays.asList(key.split(","));
        list.forEach(kv -> {
            primary.add(kv.split("=")[0]);
        });
        oggMsg.setPrimary_keys(primary);
        oggMsg.setOp_type(debeziumBean.getOp());
        oggMsg.setCurrent_ts(converDateTime(debeziumBean.getTsMs()));
        oggMsg.setOp_ts(oggMsg.getCurrent_ts());
        oggMsg.setPos(debeziumBean.getSource().getCommitLsn());
        oggMsg.setTable(String.format("%s.%s",
                debeziumBean.getSource().getDb(),
                debeziumBean.getSource().getTable()));


        return  JSON.toJSONString(oggMsg, SerializerFeature.MapSortField, SerializerFeature.WriteMapNullValue);
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ssssss");
    public static String converDateTime(String longTime){
        return sdf.format(new Date(Long.valueOf(longTime))).replace(" ","T");
    }

    public static void main(String[] args) {
        System.out.println(converDateTime("1655370802450"));
    }
}
