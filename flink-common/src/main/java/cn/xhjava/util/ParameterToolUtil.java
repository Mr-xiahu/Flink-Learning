package cn.xhjava.util;

import cn.xhjava.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class ParameterToolUtil {
    private static final Logger log = LoggerFactory.getLogger(ParameterToolUtil.class);

    public static ParameterTool createParameterTool(String[] args) {

        try {
            return ParameterTool
                    .fromPropertiesFile(ParameterToolUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromArgs(args))
                    .mergeWith(ParameterTool.fromSystemProperties());

        } catch (Exception e) {
            log.error("获取ParameterTool全局参数异常");
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }
}
