package cn.xhjava.flink_08_state.funcation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class CountWindow extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    //ValueState 使用方式，第一个字段是 count,第二个字段是运行的和
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", //状态名称
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), //类型信息
                        Tuple2.of(0L, 0L)); //状态的默认值
        sum = super.getRuntimeContext().getState(descriptor);//获取状态
    }


    @Override
    public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, Long>> collector) throws Exception {
        //访问状态的 value 值
        Tuple2<Long, Long> currentSum = sum.value();

        //更新 count
        currentSum.f0 += 1;//B

        //更新 sum
        currentSum.f1 += in.f1;//C

        //更新状态
        sum.update(currentSum);//D

        //如果 count 等于 2, 发出平均值并清除状态
        if (currentSum.f0 >= 2) {//E
            collector.collect(new Tuple2<>(in.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    /**
     * 1.数据源数据如下:
     *  Tuple2.of(1L, 3L),
     *  Tuple2.of(1L, 5L),
     *  Tuple2.of(1L, 7L),
     *  Tuple2.of(1L, 4L),
     *  Tuple2.of(1L, 2L)
     *  1.Tuple2.of(1L, 3L)第一个进来.首先执行open()方法.得到的currentSum = （0,0）
     *  2.开始执行flatmap(),经过A，B步骤后,currentSum=（1,3）.
     *      执行C步骤,更新ValueState值,也就是更新状态值.目前,状态值为:(1,3)
     *  3.第二个进来的是:Tuple2.of(1L, 5L),首先执行open()方法,获取ValueState的状态值:(1,3).
     *      紧跟着经过:A,B两步.currentSum=(2,8)
     *      并且ValueState.value的值更新为:(2,8),因为f0 >= 2,所以执行:E,结束后;currentSum=（1，4）,并且输出.清除sum，也就是清空状态.
     *  4.第三个进来的是:Tuple2.of(1L, 7L)，首先执行open()方法.因为3.中,状态被清除了,所以得到的currentSum = （0,0）
     *  5.开始执行flatmap(),经过A，B步骤后,currentSum=（1,3）.执行C步骤,更新ValueState值,也就是更新状态值.目前,状态值为:(1,7)
     *  6.第四个进来的是:Tuple2.of(1L, 4L),首先执行open()方法,获取ValueState的状态值:(1,7).紧跟着经过:A,B两步.currentSum=(2,11)
     *      并且ValueState.value的值更新为:(2,11),因为f0 >= 2,所以执行:E,结束后;currentSum=（1，5）,并且输出.清除sum，也就是清空状态.
     *
     *   ...............
     *
     */



}