package cn.xhjava.flink.hbaselookup;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author Xiahu
 * @create 2021/5/17
 */
public class Test7 {
    public static void main(String[] args) {
        System.out.println(buildCacheKey("zhangsan", "lisi", "wangwu"));
    }

    public static String buildCacheKey(Object... keys) {
        return Arrays.stream(keys)
                .map(e -> String.valueOf(e))
                .collect(Collectors.joining("_"));
    }
}

