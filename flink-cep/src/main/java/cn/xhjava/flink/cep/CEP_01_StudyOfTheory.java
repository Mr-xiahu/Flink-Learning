package cn.xhjava.flink.cep;


import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author XiaHu
 * @create 2021/11/10
 * <p>
 * CEP 理论
 */
public class CEP_01_StudyOfTheory {
    public static void main(String[] args) {
        /**
         *  1.什么是 CEP?
         *      a.复杂事件处理（Complex Event Processing，CEP）
         *      b.Flink CEP是在 Flink 中实现的复杂事件处理（CEP）库
         *      c.CEP 允许在无休止的事件流中检测事件模式，让我们有机会掌握数据中重要的部分
         *      d.一个或多个由简单事件构成的事件流通过一定的规则匹配，然后输出用户想得到的数据 —— 满足规则的复杂事件
         */


        /**
         *  2.CEP的特点
         *      目标:从有序的简单事件流中发现一些高阶的特征
         *      输入:一个或多个由简单事件构成的事件流
         *      处理:识别简单事件的联系,多个符合规则的简单事件构成复杂事件
         *      输出:满足规则的复杂事件
         *
         *  比如,现在有两条数据流内的数据如下:
         *      streamA: a a c d e a b a  -->
         *      streamB: b c d a e f a c  -->
         *
         *  首先制定规则: 事件 'a' 后紧跟事件 'b'
         *  输入: streamA streamB
         *  输入: streamA中的最早两个事件
         */


        /**
         *  3.Pattern API
         *      处理事件的规则,叫做模式(Pattern)
         *      Flink CEP提供Pattern API来处理复杂事件
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);


        //定义一个Pattern
        //该Pattern 表示,在10s的时间窗口内,如果相连的两个事件分别为: a  b 时,输出
        Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value.equals("a")) {
                            return true;
                        }
                        return false;
                    }
                })
                .next("next")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value.equals("b")) {
                            return true;
                        }
                        return false;
                    }
                }).within(Time.seconds(10));


        /**
         * 1.个体模式（Individual Patterns）
         *      组成复杂规则的每一个单独的模式定义，就是"个体模式"
         *      Pattern<String, String> start = Pattern.<String>begin("start");
         *      start.times(3).where(new SimpleCondition<String>() {...}
         *
         *   个体模式可以包括"单例（singleton）模式"和"循环（looping）模式"
         *   单例模式只接收一个事件，而循环模式可以接收多个
         *   个体模式---量词（Quantifier）
         *          可以在一个个体模式后追加量词，也就是指定循环次数
         *
         */
        Pattern<String, String> start = Pattern.<String>begin("start");
        start.times(4);//匹配出现4次
        start.times(2, 4).greedy();//匹配出现2,3或者4次,并且尽可能多的重复匹配
        start.times(0, 4).optional();//匹配出现0 或 4 次
        start.oneOrMore();//匹配出现1或多次
        start.times(2, 4);//匹配出现2,3或者4次
        start.timesOrMore(2).optional().greedy();//匹配出现0,2或者多次,并且尽可能多的重复匹配

        /**
         *  个体模式---条件(Condition)
         *      每个模式都需要指定触发条件，作为模式是否接受事件进入的判断依据
         *      CEP 中的个体模式主要通过调用 .where() .or() 和 .until() 来指定条件
         *      按不同的调用方式，可以分成以下几类：
         *          简单条件（Simple Condition）
         *              通过 .where() 方法对事件中的字段进行判断筛选，决定是否接受该事件
         *                 .where(new SimpleCondition<String>() {
         *                     @Override
         *                     public boolean filter(String value) throws Exception {
         *                         if (value.equals("b")) {
         *                             return true;
         *                         }
         *                         return false;
         *                     }
         *                 })
         *
         *          组合条件(Combining Condition)
         *              将简单条件进行合并；.or() 方法表示或逻辑相连，where 的直接组合就是 AND
         *
         */

        /**
         *
         * 2.模式组（Groups of patterns）
         *      将一个模式序列作为条件嵌套在个体模式里，成为一组模式
         *      模式序列必须由一个"初始模式"开始
         *      Pattern
         *          .<String>begin("start")
         *          .where(new SimpleCondition<String>() {...}
         *          .next("next")
         *          .where(new SimpleCondition<String>() {...}
         *
         */
    }
}