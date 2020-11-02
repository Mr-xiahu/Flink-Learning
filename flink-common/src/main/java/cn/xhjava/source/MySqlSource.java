package cn.xhjava.source;

import cn.xhjava.domain.Rule;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class MySqlSource extends RichSourceFunction<Rule> {
    private static final Logger log = LoggerFactory.getLogger(MySqlSource.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());

        String database = parameterTool.get("database");
        String host = parameterTool.get("host");
        String password = parameterTool.get("password");
        String port = parameterTool.get("port");
        String username = parameterTool.get("username");

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);

        if (connection != null) {
            String sql = "select * from alarm_notify";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Rule> ctx) throws Exception {
        Rule rule = null;
        Map<String, String> map = new HashMap<>();
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if (0 == resultSet.getInt("status")) {
                    map.put(resultSet.getString("type") + resultSet.getString("type_id"), resultSet.getString("target_id"));
                    rule = new Rule(resultSet.getString("target_id"), resultSet.getString("type"));
                }
            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", map.size(), map);

            ctx.collect(rule);
            map.clear();
            Thread.sleep(2000 * 60);
        }

    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            log.error("runException:{}", e);
        }
        isRunning = false;
    }
}
