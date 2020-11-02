#### Flink Window理解

#### 一. 什么是window

~~~
Flink是流处理框架，所以在了解window之前,我们先了解一下“流”是什么是必不可少的.
看一下官网介绍:
Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams
flink是一个用于计算有界限的流和无界限流的一个框架和处理引擎。
~~~

~~~
在生活中最常见的流:河流.这个流里面装的都是水.
我们现在要处理的是流是数据流:里面装的都是数据.
有界限流:比如乡下的一个小河流,这条流是有界限的.
无界限流:黄河,长江.其实这两个流也是有界限的,但是因为它们太长的,假设它们为无界限的.
~~~

~~~
目前有许多数据分析的场景从批处理到流处理的演变， 虽然可以将批处理作为流处理的特殊情况来处理，但是分析无穷集的流数据通常需要思维方式的转变并且具有其自己的术语，例如，“windowing（窗口化）”、“at-least-once（至少一次）”、“exactly-once（只有一次）” 。
~~~

~~~
对于刚刚接触流处理的人来说，这种转变和新术语可能会非常混乱。 Apache Flink 是一个为生产环境而生的流处理器，具有易于使用的 API，可以用于定义高级流分析程序。Flink 的 API 在数据流上具有非常灵活的窗口定义，使其在其他开源流处理框架中脱颖而出。
~~~

##### 1.1 通过一个通俗的例子来介绍window

~~~
现在存在一个流:河流.但是呢,我们现在要统计这条河流里面的鱼的数量.
假如：这条河流是一条有界限流.假设现在这条河流就是你家乡的那条小河.你现在该怎么统计?
很简单,因为是有界限的流,我们直接给这条小河的水抽干,在后在统计(批处理)
在假如：这条河流是一条无界限流.假设是长江,黄河中的任意一条.你又该怎么统计?
这次你还能抽水吗?显然不可以.
但是我可以将其分为N段来统计.比如说:我使用渔网将流划分为N段
第一段:河流源头------->第一段尾(首尾相连)
第二段:第二段头------->第二段尾
第三段:第二段头------->第三段尾
.
.
.
第n段:第n段头------->第n段尾
此时,我们只需要分别统计每一段河流里面的鱼的数据即可.
这个每一段,就是window(窗口)
~~~

##### 1.2 window官方版定义

~~~
Window 就是用来对一个无限的流设置一个有限的集合，在有界的数据集上进行操作的一种机制
~~~

##### 1.3 window的种类

~~~
以时间驱动的 Time Window
以事件数量驱动的 Count Window
以会话间隔驱动的 Session Window
~~~

#### 二.window的种类

##### 2.1 普通时间窗口

~~~java
public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用socketTextStream作为dataSource,接收数据
         *      2.使用flatMap将接收的word封装为成元组Tuple2<String,Integre>
         *      3.根据word进行分组,将word相同的分为同一组
         *      4.使用时间窗口:timeWindow,每隔30s做一个sum,并打印
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.112", 7887);
        //使用算子解析
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\W+");
                for (String sttr : split) {
                    collector.collect(new Tuple2<>(sttr, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(30)).sum(1).print();
        env.execute();
    }
~~~

##### 2.2 滑动时间窗口

~~~java
public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用socketTextStream作为dataSource,接收数据
         *      2.使用flatMap将接收的word封装为成元组Tuple2<String,Integre>
         *      3.根据word进行分组,将word相同的分为同一组
         *      4.使用时间窗口:timeWindow,每隔15s,统计过去30s的word数量之和并打印
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.112", 7887);
        //使用算子解析
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\W+");
                for (String sttr : split) {
                    collector.collect(new Tuple2<>(sttr, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(30), Time.seconds(15)).sum(1).print();
        env.execute();
    }
~~~

##### 2.3 普通事件数量窗口

~~~java
public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用fromElements 作为数据源
         *      2.使用flatMap过滤 f0 >5 的元组
         *      3.根据 f1 进行分组,将 f1 相同的分为同一组
         *      4.使用数量窗口:countWindow,累计2个, 做一次sum ,并打印
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<Tuple3<Integer, String, Long>> dataStream = env.fromElements(
                new Tuple3<>(1, "zhangsan", 18l),
                new Tuple3<>(2, "lisi", 17l),
                new Tuple3<>(3, "wangwu", 20l),
                new Tuple3<>(4, "zhaoliu", 25l),
                new Tuple3<>(5, "xiahu", 22l),
                new Tuple3<>(5, "xiahu", 25l),
                new Tuple3<>(6, "yangming", 21l),
                new Tuple3<>(1, "zhangsan", 18l),
                new Tuple3<>(2, "lisi", 17l),
                new Tuple3<>(3, "wangwu", 20l),
                new Tuple3<>(4, "zhaoliu", 25l),
                new Tuple3<>(5, "xiahu", 22l),
                new Tuple3<>(5, "xiahu", 25l),
                new Tuple3<>(6, "yangming", 21l)
        );
        //使用算子解析
        dataStream.flatMap(new FlatMapFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>>() {
            @Override
            public void flatMap(Tuple3<Integer, String, Long> tuple, Collector<Tuple3<Integer, String, Long>> collector) throws Exception {
                if (tuple.f0 <= 5 && tuple.f0 >= 0) {
                    collector.collect(tuple);
                }
            }
        }).keyBy(1).countWindow(2).sum(0).print();


        env.execute();
    }
~~~

##### 2.4 滑动事件数量窗口

~~~java
public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用fromElements 作为数据源
         *      2.使用flatMap过滤 f0 >5 的元组
         *      3.根据 f1 进行分组,将 f1 相同的分为同一组
         *      4.使用数量窗口:countWindow,每隔 1 个元组,将之前的 2 个元组的 f0 做一次 sum,将元组的 f0 做一次sum ,并打印
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<Tuple3<Integer, String, Long>> dataStream = env.fromElements(
                new Tuple3<>(1, "zhangsan", 18l),
                new Tuple3<>(2, "lisi", 17l),
                new Tuple3<>(3, "wangwu", 20l),
                new Tuple3<>(4, "zhaoliu", 25l),
                new Tuple3<>(5, "xiahu", 22l),
                new Tuple3<>(5, "xiahu", 25l),
                new Tuple3<>(6, "yangming", 21l),
                new Tuple3<>(1, "zhangsan", 18l),
                new Tuple3<>(2, "lisi", 17l),
                new Tuple3<>(3, "wangwu", 20l),
                new Tuple3<>(4, "zhaoliu", 25l),
                new Tuple3<>(5, "xiahu", 22l),
                new Tuple3<>(5, "xiahu", 25l),
                new Tuple3<>(6, "yangming", 21l)
        );
        //使用算子解析
        dataStream.flatMap(new FlatMapFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>>() {
            @Override
            public void flatMap(Tuple3<Integer, String, Long> tuple, Collector<Tuple3<Integer, String, Long>> collector) throws Exception {
                if (tuple.f0 <= 5 && tuple.f0 >= 0) {
                    collector.collect(tuple);
                }
            }
        }).keyBy(1).countWindow(2, 1).sum(0).print();


        env.execute();
    }
~~~

##### 2.5 会话窗口

~~~java
public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用socketTextStream作为dataSource,接收数据
         *      2.使用flatMap将接收的word封装为成元组Tuple2<String,Integre>
         *      3.根据word进行分组,将word相同的分为同一组
         *      4.使用会话窗口:如果 30s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.112", 7887);
        //使用算子解析
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\W+");
                for (String sttr : split) {
                    collector.collect(new Tuple2<>(sttr, 1));
                }
            }
        }).keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(30))).sum(1).print();
        env.execute();
    }
~~~

#### 三.window自定义窗口

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-073301.png)

##### 3.1 Window 源码定义

上面说了 Flink 中自带的 Window，主要利用了 KeyedStream 的 API 来实现，我们这里来看下 Window 的源码定义如下：

```java
public abstract class Window {
    //获取属于此窗口的最大时间戳
    public abstract long maxTimestamp();
}
```

查看源码可以看见 Window 这个抽象类有如下实现类：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-163050.png)

###### TimeWindow源码:

```java
public class TimeWindow extends Window {
    //窗口开始时间
    private final long start;
    //窗口结束时间
    private final long end;
}
```

###### GlobalWindow 源码:

```java
public class GlobalWindow extends Window {

    private static final GlobalWindow INSTANCE = new GlobalWindow();

    private GlobalWindow() { }
    //对外提供 get() 方法返回 GlobalWindow 实例，并且是个全局单例
    public static GlobalWindow get() {
        return INSTANCE;
    }
}
```

##### 3.2  WindowAssigner

到达窗口操作符的元素被传递给 WindowAssigner。WindowAssigner 将元素分配给一个或多个窗口，可能会创建新的窗口。

窗口本身只是元素列表的标识符，它可能提供一些可选的元信息，例如 TimeWindow 中的开始和结束时间。注意，元素可以被添加到多个窗口，这也意味着一个元素可以同时在多个窗口存在。我们来看下 WindowAssigner 的代码的定义吧：

```java
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
    //分配数据到窗口并返回窗口集合
    public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);
}
```

查看源码可以看见 WindowAssigner 这个抽象类有如下实现类：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-163413.png)

这些 WindowAssigner 实现类的作用介绍：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-16-155715.jpg)

如果你细看了上面图中某个类的具体实现的话，你会发现一个规律，比如我拿 TumblingEventTimeWindows 的源码来分析，如下：

```java
public class TumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
    //定义属性
    private final long size;
    private final long offset;

    //构造方法
    protected TumblingEventTimeWindows(long size, long offset) {
        if (Math.abs(offset) >= size) {
            throw new IllegalArgumentException("TumblingEventTimeWindows parameters must satisfy abs(offset) < size");
        }
        this.size = size;
        this.offset = offset;
    }

    //重写 WindowAssigner 抽象类中的抽象方法 assignWindows
    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        //实现该 TumblingEventTimeWindows 中的具体逻辑
    }

    //其他方法，对外提供静态方法，供其他类调用
}
```

从上面你就会发现**套路**：

1、定义好实现类的属性

2、根据定义的属性添加构造方法

3、重写 WindowAssigner 中的 assignWindows 等方法

4、定义其他的方法供外部调用

##### 3.3 Trigger

Trigger 表示触发器，每个窗口都拥有一个 Trigger（触发器），该 Trigger 决定何时计算和清除窗口。当先前注册的计时器超时时，将为插入窗口的每个元素调用触发器。在每个事件上，触发器都可以决定触发，即清除（删除窗口并丢弃其内容），或者启动并清除窗口。一个窗口可以被求值多次，并且在被清除之前一直存在。注意，在清除窗口之前，窗口将一直消耗内存。

说了这么一大段，我们还是来看看 Trigger 的源码，定义如下：

```java
public abstract class Trigger<T, W extends Window> implements Serializable {
    //当有数据进入到 Window 运算符就会触发该方法
    public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;
    //当使用触发器上下文设置的处理时间计时器触发时调用
    public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;
    //当使用触发器上下文设置的事件时间计时器触发时调用该方法
    public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;
}
```

当有数据流入 Window 运算符时就会触发 onElement 方法、当处理时间和事件时间生效时会触发 onProcessingTime 和 onEventTime 方法。每个触发动作的返回结果用 TriggerResult 定义。继续来看下 TriggerResult 的源码定义：

```java
public enum TriggerResult {

    //不做任何操作
    CONTINUE(false, false),

    //处理并移除窗口中的数据
    FIRE_AND_PURGE(true, true),

    //处理窗口数据，窗口计算后不做清理
    FIRE(true, false),

    //清除窗口中的所有元素，并且在不计算窗口函数或不发出任何元素的情况下丢弃窗口
    PURGE(false, true);
}
```

查看源码可以看见 Trigger 这个抽象类有如下实现类：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-163751.png)

这些 Trigger 实现类的作用介绍：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-17-145735.jpg)

如果你细看了上面图中某个类的具体实现的话，你会发现一个规律，拿 CountTrigger 的源码来分析，如下：

```java
public class CountTrigger<W extends Window> extends Trigger<Object, W> {
    //定义属性
    private final long maxCount;

    private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);
    //构造方法
    private CountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    //重写抽象类 Trigger 中的抽象方法 
    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        //实现 CountTrigger 中的具体逻辑
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }
}
```

**套路**：

1. 定义好实现类的属性
2. 根据定义的属性添加构造方法
3. 重写 Trigger 中的 onElement、onEventTime、onProcessingTime 等方法
4. 定义其他的方法供外部调用

##### 3.4 Evictor

Evictor 表示驱逐者，它可以遍历窗口元素列表，并可以决定从列表的开头删除首先进入窗口的一些元素，然后其余的元素被赋给一个计算函数，如果没有定义 Evictor，触发器直接将所有窗口元素交给计算函数。

我们来看看 Evictor 的源码定义如下：

```java
public interface Evictor<T, W extends Window> extends Serializable {
    //在窗口函数之前调用该方法选择性地清除元素
    void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
    //在窗口函数之后调用该方法选择性地清除元素
    void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
}
```

查看源码可以看见 Evictor 这个接口有如下实现类：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-163942.png)

这些 Evictor 实现类的作用介绍：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-17-153505.jpg)

如果你细看了上面三种中某个类的实现的话，你会发现一个规律，比如我就拿 CountEvictor 的源码来分析，如下：

```java
public class CountEvictor<W extends Window> implements Evictor<Object, W> {
    private static final long serialVersionUID = 1L;

    //定义属性
    private final long maxCount;
    private final boolean doEvictAfter;

    //构造方法
    private CountEvictor(long count, boolean doEvictAfter) {
        this.maxCount = count;
        this.doEvictAfter = doEvictAfter;
    }
    //构造方法
    private CountEvictor(long count) {
        this.maxCount = count;
        this.doEvictAfter = false;
    }

    //重写 Evictor 中的 evictBefore 方法
    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            //调用内部的关键实现方法 evict
            evict(elements, size, ctx);
        }
    }

    //重写 Evictor 中的 evictAfter 方法
    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            //调用内部的关键实现方法 evict
            evict(elements, size, ctx);
        }
    }

    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        //内部的关键实现方法
    }

    //其他的方法
}
```

发现**套路**：

1. 定义好实现类的属性
2. 根据定义的属性添加构造方法
3. 重写 Evictor 中的 evictBefore 和 evictAfter 方法
4. 定义关键的内部实现方法 evict，处理具体的逻辑
5. 定义其他的方法供外部调用

上面我们详细讲解了 Window 中的组件 WindowAssigner、Trigger、Evictor，然后继续回到问题：如何自定义 Window？

上文讲解了 Flink 自带的 Window（Time Window、Count Window、Session Window），然后还分析了他们的源码实现，通过这几个源码，我们可以发现，它最后调用的都有一个方法，那就是 Window 方法，如下：

```java
//提供自定义 Window
public <W extends Window> WindowedStream<T, KEY, W> window(WindowAssigner<? super T, W> assigner) {
    return new WindowedStream<>(this, assigner);
}

//构造一个 WindowedStream 实例
public WindowedStream(KeyedStream<T, K> input,
        WindowAssigner<? super T, W> windowAssigner) {
    this.input = input;
    this.windowAssigner = windowAssigner;
    //获取一个默认的 Trigger
    this.trigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
}
```

可以看到这个 Window 方法传入的参数是一个 WindowAssigner 对象（你可以利用 Flink 现有的 WindowAssigner，也可以根据上面的方法来自定义自己的 WindowAssigner），然后再通过构造一个 WindowedStream 实例（在构造实例的会传入 WindowAssigner 和获取默认的 Trigger）来创建一个 Window。

另外你可以看到滑动计数窗口，在调用 window 方法之后，还调用了 WindowedStream 的 evictor 和 trigger 方法，trigger 方法会覆盖掉你之前调用 Window 方法中默认的 trigger，如下：

```java
//滑动计数窗口
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
    return window(GlobalWindows.create()).evictor(CountEvictor.of(size)).trigger(CountTrigger.of(slide));
}

//trigger 方法
public WindowedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
    if (windowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
        throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
    }

    if (windowAssigner instanceof BaseAlignedWindowAssigner) {
        throw new UnsupportedOperationException("Cannot use a " + windowAssigner.getClass().getSimpleName() + " with a custom trigger.");
    }
    //覆盖之前的 trigger
    this.trigger = trigger;
    return this;
}
```

从上面的各种窗口实现，你就会发现了：Evictor 是可选的，但是 WindowAssigner 和 Trigger 是必须会有的，这种创建 Window 的方法充分利用了 KeyedStream 和 WindowedStream 的 API，再加上现有的 WindowAssigner、Trigger、Evictor，你就可以创建 Window 了，另外你还可以自定义这三个窗口组件的实现类来满足你公司项目的需求。

### 小结与反思

本节从生活案例来分享关于 Window 方面的需求，进而开始介绍 Window 相关的知识，并把 Flink 中常使用的三种窗口都一一做了介绍，并告诉大家如何使用，还分析了其实现原理。最后还对 Window 的内部组件做了详细的分析，为自定义 Window 提供了方法。