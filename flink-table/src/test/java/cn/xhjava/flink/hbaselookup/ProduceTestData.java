package cn.xhjava.flink.hbaselookup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021/4/9
 */
public class ProduceTestData {
    static String line = "%s,%s,%s";

    public static void main(String[] args) throws IOException {
        int count = 1000000;
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\1000000")));
        for (int i = 0; i <= count; i++) {
            String line = String.format(ProduceTestData.line, i, "english", "beijing");
            writer.write(line);
            writer.newLine();
            writer.flush();
        }

        writer.close();
    }
}
