package cn.xhjava.flink.table.udf.tablefuncation;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021/4/16
 * <p>
 * lookup redis return string
 */
public class RedisLookupFunction extends TableFunction<String> {


    private Jedis jedis;
    private String tableName;


    public RedisLookupFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        jedis = new Jedis("node2");

    }

    public void eval(Object rowKey) throws IOException {
        String rowKey1 = (String) rowKey;
        String value = jedis.get(tableName + "_" + rowKey1);
        //collect(parseRow(value, (String) rowKey, tableName));
        collect(value);
    }


    //id:1,name:zhangsan,age:4
    /*private static Row parseRow(String value, String rowkey, String tableName) {
        String[] fieldMap = value.split(",");
        Row row = new Row(fieldMap.length + 1);
        row.setField(1, rowkey);
        Row familyRow = familyRows[f];
        for (int i = 0; i < fieldMap.length; i++) {

        }
        return null;
    }*/

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }

    public static void main(String[] args) {
        //连接本地的 Redis 服务
        Jedis jedis = new Jedis("node2");
        // 如果 Redis 服务设置来密码，需要下面这行，没有就不需要
        // jedis.auth("123456");
        System.out.println("连接成功");
        //查看服务是否运行
        System.out.println("服务正在运行: " + jedis.ping());
        jedis.set("xiahu", "真帅啊！");
        String xiahu = jedis.get("xiahu");
        System.out.println(xiahu);
        jedis.close();
    }
}