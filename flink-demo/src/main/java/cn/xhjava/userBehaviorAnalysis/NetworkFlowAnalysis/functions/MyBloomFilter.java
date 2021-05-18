package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions;

/**
 * @author Xiahu
 * @create 2021-05-18
 */
// 自定义一个布隆过滤器
public class MyBloomFilter {
    // 定义位图的大小，一般需要定义为2的整次幂
    private Integer cap;

    public MyBloomFilter(Integer cap) {
        this.cap = cap;
    }

    // 实现一个hash函数
    public Long hashCode(String value, Integer seed) {
        Long result = 0L;
        for (int i = 0; i < value.length(); i++) {
            result = result * seed + value.charAt(i);
        }
        return result & (cap - 1);
    }
}
