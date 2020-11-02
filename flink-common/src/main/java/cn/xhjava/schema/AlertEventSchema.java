package cn.xhjava.schema;

import cn.xhjava.domain.AlertEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2020/1/6
 * @since 1.0.0
 */
public class AlertEventSchema implements DeserializationSchema<AlertEvent> {


    @Override
    public AlertEvent deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(AlertEvent alertEvent) {
        return false;
    }

    @Override
    public TypeInformation<AlertEvent> getProducedType() {
        return null;
    }
}