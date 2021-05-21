package cn.xhjava.flink.stream.transfromfunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author Xiahu
 * @create 2021-05-21
 */
public class RowToRowDataMapFunction implements MapFunction<Row, RowData> {
    private static final Logger log = LoggerFactory.getLogger(RowToRowDataMapFunction.class);

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    public RowToRowDataMapFunction(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public RowData map(Row row) throws Exception {
        //RowData封装
        String[] fieldDatas = row.toString().split(",");
        if (fieldDatas.length == fieldNames.length) {
            int arity = fieldDatas.length;
            GenericRowData rowData = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String field = fieldDatas[i];
                BinaryStringData binaryStringData = new BinaryStringData(field);
                rowData.setField(i, binaryStringData);
                rowData.setRowKind(row.getKind());
            }
            return rowData;
        } else {
            log.error("field length is not same! \n fileNames ：{} \nfieldData : {}", Arrays.toString(fieldNames), Arrays.toString(fieldDatas));
        }

        return null;
    }
}
