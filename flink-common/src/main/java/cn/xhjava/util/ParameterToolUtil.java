package cn.xhjava.util;

import cn.xhjava.constant.FlinkLearnConstant;
import cn.xhjava.constant.PropertiesConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class ParameterToolUtil {
    private static final Logger log = LoggerFactory.getLogger(ParameterToolUtil.class);

    public static ParameterTool createParameterTool(String[] args) {
        if (args.length <= 0) {
            log.error("Please check config path");
            System.exit(0);
        }

        try {
            ParameterTool partmeter = ParameterTool
                    .fromPropertiesFile(new FileInputStream(new File(args[0])));
//                    .fromPropertiesFile(ParameterToolUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.PROPERTIES_KAFKA_FILE_NAME))
            //.mergeWith(ParameterTool.fromArgs(args));
            if (StringUtils.isNotEmpty(partmeter.get(FlinkLearnConstant.KAFKA_CONFIG_PATH))) {
                partmeter = partmeter.mergeWith(
                        ParameterTool.fromPropertiesFile(
                                new FileInputStream(
                                        new File(
                                                partmeter.get(FlinkLearnConstant.KAFKA_CONFIG_PATH)))));
            }

            if (StringUtils.isNotEmpty(partmeter.get(FlinkLearnConstant.KERBEROS_CONFIG_PATH))) {
                partmeter = partmeter.mergeWith(
                        ParameterTool.fromPropertiesFile(
                                new FileInputStream(
                                        new File(
                                                partmeter.get(FlinkLearnConstant.KERBEROS_CONFIG_PATH)))));
            }

            return partmeter;
        } catch (Exception e) {
            log.error("获取ParameterTool全局参数异常");
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }
}
