package cn.xhjava.flink.cep;


import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

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
         *
         */

        /**
         *
         *  个体模式---量词（Quantifier）
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
         *              将简单条件进行合并；.or() 方法表示或逻辑相连,where 的直接组合就是 AND
         *
         *          终止条件(Stop Condition)
         *              如果使用了oneOrMore()或者timesOrMore().optional(),建议使用'.until()'作为终止条件,以便于清理状态
         *
         *          迭代条件(Iterative Condition)
         *              能够对之前接收的所有事件进行处理
         *              可以调用ctx.getEventsForPattern("name").where(new IterativeCondition<Event>(){...})
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


        /**
         *  3.模式序列
         *      严格近邻(Strict Contiguity)
         *          所有事件按照严格的顺序出现,中间没有任何不匹配事件,由'next()'指定
         *          例如对于模式' a next b',事件序列[a,c,b1,b2]没有匹配;事件序列[c,c2,b,a] -->  可以匹配
         *
         *      宽松近邻(Relaxed Contiguity)
         *          允许中间出现不匹配的事件,由'.followedBy()'指定
         *          例如对于模式'a followedBy b',事件序列[a,c,b1,b2] 匹配未：a,b1
         *
         *      非确定性宽松近邻(Non-Deterministic Relaxed Contiguity)
         *          进一步放宽条件,之前已经匹配过的事件也可以再次使用,由'.followedByAndy'指定
         *          例如对于模式'a followedByAny b',事件序列[a,c,b1,b2]匹配为:[a,b1],[a,b2]
         *
         *      除去以上模式序列外,还可以定义"不希望出现某种近邻关系":
         *          .notNext() : 不想让某个事件严格紧邻前一个事件发生
         *          .notFollowedBy() :  不想让某个事件在两个事件中发生
         *
         *      注意：
         *          所有模式序列必须以 .begin() 开始
         *          模式序列不能以 .followedBy 结束
         *          'not' 类型的模式不能呗 optional 所修饰
         *          可以使用 .within(Time.seconds(10)) 为模式指定时间约束,用来要求在多长时间内有效(这里表示10s的时间内有效)
         */


        /**
         *  模式的检测
         *      指定要查找的模式序列后,将其应用于输入数据流上进行检测潜在的匹配
         *      比如：
         */
        Pattern<String, String> pattern = Pattern.<String>begin("start")
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
        PatternStream<String> patternStream = CEP.pattern(stream, pattern);


        /**
         *  匹配事件的提取
         *      创建PatternStream后可以使用select()或者flatSelect()方法,从检测到的事件中提取事件
         *      select()需要输入一个select function作为参数,每个成功匹配的事件序列都会调用它
         *
         */
        SingleOutputStreamOperator<String> selectStream = patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                return pattern.get("start").get(0);
            }
        });


        /**
         *  超时事件提取
         *    当一个模式通过within 关键字定义了检测窗口事件时,部分事件序列可能因为超过窗口长度而被丢弃,为了能够处理这些超时的部分匹配,
         *          select 和 flatSelect API调用允许指定超时处理程序
         *    超时处理程序会接收到目前为止由模式匹配到的所有事件,由一个OutputTag定义接收到的超时事件
         */

        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<String, String>() {
            @Override
            public String timeout(Map<String, List<String>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("start").get(0);
            }
        }, new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                return pattern.get("start").get(0);
            }
        });

        DataStream<String> timeOutStream = selectDS.getSideOutput(timeOutTag);


    }
}