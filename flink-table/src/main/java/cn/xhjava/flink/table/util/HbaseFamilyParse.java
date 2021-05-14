package cn.xhjava.flink.table.util;

import org.apache.flink.connector.hbase.util.HBaseTableSchema;

/**
 * @author Xiahu
 * @create 2021/4/9
 */
public class HbaseFamilyParse {
    public static HBaseTableSchema parseHBaseTableSchema(String line, String familyColumn) {
        HBaseTableSchema baseTableSchema = new HBaseTableSchema();
        baseTableSchema.setRowKey("rowkey", String.class);
        String[] field = line.split(",");
        for (String column : field) {
            baseTableSchema.addColumn(familyColumn, column, String.class);
        }
        return baseTableSchema;
    }
}
