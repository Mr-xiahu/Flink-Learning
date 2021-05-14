package cn.xhjava.schema;

import cn.xhjava.domain.OggMsg;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Xiahu
 * @create 2020/11/26
 */
@Slf4j
public class OggMsgSchema implements DeserializationSchema<OggMsg> {


    //这里返回一个OggMsg类型的数据
    @Override
    public OggMsg deserialize(byte[] message) throws IOException {
        OggMsg result = null;
        String msg = new String(message, StandardCharsets.UTF_8);
        try {
            result = JSONObject.parseObject(msg, OggMsg.class);

        } catch (Exception e) {
            log.warn("存在脏数据 {}", msg);
        }
        return result;
    }


    //是否表示流的最后一条元素,设置为false，表示数据会源源不断的到来
    @Override
    public boolean isEndOfStream(OggMsg nextElement) {
        return false;
    }

    //指定数据的输入类型
    @Override
    public TypeInformation<OggMsg> getProducedType() {
        return TypeInformation.of(OggMsg.class);
    }
}
