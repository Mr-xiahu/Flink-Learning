package cn.xhjava.util;

import cn.xhjava.constant.NuwaConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/9/1
 */
public class MysqlUtil {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlUtil.class);

    private String url;
    private String user;
    private String pass;
    private String database;
    private String tableName;
    private Connection connection;
    private Statement statement;
    private ResultSet rs;


    public MysqlUtil(Properties prop) {
        this.url = prop.getProperty(NuwaConstant.NUWA_UTIL_DATA_SCHEMA_URL);
        this.user = prop.getProperty(NuwaConstant.NUWA_UTIL_DATA_SCHEMA_USER);
        this.pass = prop.getProperty(NuwaConstant.NUWA_UTIL_DATA_SCHEMA_PASS);
        this.database = prop.getProperty(NuwaConstant.NUWA_UTIL_DATA_SCHEMA_DATABASE);
        this.tableName = prop.getProperty(NuwaConstant.NUWA_UTIL_DATA_SCHEMA_TABLE);
        try {
            this.buildDbConnection();
        } catch (Exception e) {
            LOG.error(String.format("Build MySQL Connection Error \n%s", ExceptionUtil.getStackTrace(e)));
        }
    }


    public MysqlUtil(Properties prop, Boolean isPartitionDb) {
        this.url = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_URL);
        this.user = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_USER);
        this.pass = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_PASS);
        this.database = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_DATABASE);
        this.tableName = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_TABLE);
        try {
            this.buildDbConnection();
        } catch (Exception e) {
            LOG.error(String.format("Build MySQL Connection Error \n%s", ExceptionUtil.getStackTrace(e)));
        }
    }


    public ResultSet executQuery(String sql) {
        try {
            rs = statement.executeQuery(sql);
        } catch (Exception e) {
            LOG.error(String.format("Execut SQL:【 %s 】 Error \n%s", sql, ExceptionUtil.getStackTrace(e)));
        }
        return rs;
    }

    public Boolean executSql(String sql) {
        Boolean result = true;
        try {
            statement.execute(sql);
        } catch (Exception e) {
            result = false;
            LOG.error(String.format("Execut SQL:【 %s 】 Error \n%s", sql, ExceptionUtil.getStackTrace(e)));
        }

        return result;
    }

    public MysqlUtil(Properties prop, String flag) {
        if (flag.equals(NuwaConstant.NUWA_UTIL_FLAG)) {
            this.url = prop.getProperty(NuwaConstant.NUWA_UTIL_STORAGE_METADATA_URL);
            this.user = prop.getProperty(NuwaConstant.NUWA_UTIL_STORAGE_METADATA_USER);
            this.pass = prop.getProperty(NuwaConstant.NUWA_UTIL_STORAGE_METADATA_PASS);
            /*this.database = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_DATABASE);
            this.tableName = prop.getProperty(NuwaConstant.NUWA_IMPORT_PARTITION_COFIG_TABLE);*/
            try {
                this.buildDbConnection();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(String.format("Build MySQL Connection Error \n%s", ExceptionUtil.getStackTrace(e)));
            }
        }
    }


    private void buildDbConnection() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(url, user, pass);
        statement = connection.createStatement();
    }

    public Map<String, String> getDbAndPathMap() {
        Map<String, String> map = new HashMap<>();
        String sql = String.format("select * from %s.%s", database, tableName);
        try {
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                map.put(rs.getString("datalake_name"), rs.getString("datalake_path"));
            }
        } catch (SQLException e) {
            LOG.error(String.format("SQL : %s Query Error \n", sql, ExceptionUtil.getStackTrace(e)));
        } finally {
            close();
        }
        return map;
    }

    public void close() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.error(String.format("Mysql Connection Resource Close Error \n", ExceptionUtil.getStackTrace(e)));
        }

    }

}
