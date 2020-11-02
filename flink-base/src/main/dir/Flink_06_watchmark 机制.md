### Flink watermark 机制

#### 一.watermark是什么

~~~
在Flink程序中,如果处理数据的类型不是特别重视数据自身携带的时间,那么使用ProessingTime(Flink默认使用的事件语义)是完全没问题的.比如说:OGG的增量数据.通常处理这类数据,只需要不漏数据即可.
而那些特别重视事件自身携带时间的那类数据,这个时候就需要使用EventTime时间语义(需要自己设定).
由于网络波动,机器故障等一些原因,你会遇到:事件乱序 & 事件延迟 这些问题.但是你现在处理的这部分数据需要保证这些数据的顺序.比如:错误日志的时间戳.
庆幸的是 Flink 支持用户以事件时间来定义窗口（也支持以处理时间来定义窗口），那么这样就要去解决上面所说的两个问题。针对上面的问题（事件乱序 & 事件延迟），Flink 引入了 Watermark 机制来解决.
~~~

##### 1.1 简单理解watermark

~~~java
//概念:watermark 理解成为一个时间戳.如果现在要处理的数据类型为Word
private Integer id;
private Integer name;
//id,name为Word的两个属性.我准备给这个加watchmark,所以我在添加一个属性:timeStramp
private long timeStramp;
~~~

​	![图解watermark]()

~~~
1.dataStream流里面都是数据.假设每个数据都是一个Word,其中有如上介绍三种属性
2.存在两个window.window1处理watermark 1---10之间的数据.window2处理 11-----20之间的数据
3.第一个数据来了.它的watermark为1,所以进入window1中等待计算
4.第二个数据来了,它的watermark为2, 2 介于 1 ---10之间.所以进入window1中等待计算
5.第三个数据来了,它的watermark为11, 11 介于 11---20之间,所以进入window2中等待计算.并且在此时:
window1认为watermark介于1----10的数据全部存在完毕.所以开始计算(后续操作).
6.window2继续等待下一个数据.假如第四个数据的watermark >20 ，那么第四个数据进入window3中等待计算.window2开始计算.
~~~

以上就是对于watermark工作原理.通俗来说:

~~~
现在有25名学生.他们的年纪分别是1,2,3,4...25.
幼儿园,小学,中学,大学,博士开始录取学生.
1.幼儿园首先开始录取:年纪为1---6岁的学生可以入学.年龄排序如下的学生依次入学:1,2,3,4,5,6
2.当6岁的学生入学之后,幼儿园方面认为,已经没有其他的学生来了.所以关闭报名通道,准备授课......
3.此时小学开始招生:7---12周岁的学生依次去报名:7,8,9,10,15,11,12
4.年龄为:7,8,9,10的学生顺利入学.到15岁的学生来了,小学方面认为:你的年龄太大了,超出的我的限制,你去初中部报名.
于此同时,还认为:既然这么大的学生都了,后续应该没有年龄介于7--12岁的学生.所以关闭招生通道,准备上课.
5.那后面的11,12岁的学生要上小学怎么办呢?flink 有一个延迟等待机制可以设置,在这个范围内,你还是可以入学报名的.
比如我设置为:1天,结果下午11，12就来了,他们可以顺利入学.如果这两名同学住的比较远,在一天的时间内没有赶到又怎么办呢?
学校不收,直接抛弃
6.......
7.......
...
..
.
~~~

#### 二.Flink watermark的使用

##### 2.1 两种方式

~~~java
/*
*	AssignerWithPunctuatedWatermarks: 数据流中每一个递增的 EventTime 都会产生一个 Watermark。
*	在实际的生产环境中，在 TPS 很高的情况下会产生大量的 Watermark，可能在一定程度上会对下游算子造成一定的压力, *	  所以只有在实时性要求非常高的场景才会选择这种方式来进行水印的生成。
*/
public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> timestampAndWatermarkAssigner) {

    final int inputParallelism = getTransformation().getParallelism();
    final AssignerWithPeriodicWatermarks<T> cleanedAssigner = clean(timestampAndWatermarkAssigner);

    TimestampsAndPeriodicWatermarksOperator<T> operator = new TimestampsAndPeriodicWatermarksOperator<>(cleanedAssigner);

    return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator).setParallelism(inputParallelism);
}


/*
 *	AssignerWithPeriodicWatermarks：周期性的（一定时间间隔或者达到一定的记录条数）产生一个 Watermark。
 *	在实际的生产环境中，通常这种使用较多，它会周期性产生 Watermark 的方式,但是必须结合时间或者积累条数两个维      *   度，否则在极端情况下会有很大的延时，所以 Watermark 的生成方式需要根据业务场景的不同进行不同的选择。
 */
public SingleOutputStreamOperator<T>assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> timestampAndWatermarkAssigner) {

    final int inputParallelism = getTransformation().getParallelism();
    final AssignerWithPunctuatedWatermarks<T> cleanedAssigner = clean(timestampAndWatermarkAssigner);

    TimestampsAndPunctuatedWatermarksOperator<T> operator = new TimestampsAndPunctuatedWatermarksOperator<>(cleanedAssigner);

    return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator).setParallelism(inputParallelism);
}
~~~

##### 2.2 Punctuated Watermark

AssignerWithPunctuatedWatermarks 接口中包含了 checkAndGetNextWatermark 方法，这个方法会在每次 extractTimestamp() 方法被调用后调用，它可以决定是否要生成一个新的水印，返回的水印只有在不为 null 并且时间戳要大于先前返回的水印时间戳的时候才会发送出去，如果返回的水印是 null 或者返回的水印时间戳比之前的小则不会生成新的水印。

需要注意的是这种情况下可以为每个事件都生成一个水印，但是因为水印是要在下游参与计算的，所以过多的话会导致整体计算性能下降。

~~~java
public class WordPunctuatedWatermark implements AssignerWithPunctuatedWatermarks<Word> {

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Word lastElement, long extractedTimestamp) {
        return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(Word element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
~~~

##### 2.3 Periodic Watermark

通常在生产环境中使用 AssignerWithPeriodicWatermarks 来定期分配时间戳并生成水印比较多，那么先来讲下这个该如何使用。

```java
public class WordWatermark implements AssignerWithPeriodicWatermarks<Word> {

    private long currentTimestamp = Long.MIN_VALUE;
	//从数据本身中提取 Event Time，该方法会返回当前时间戳与事件时间进行比较，如果事件的时间戳比 					currentTimestamp 大的话，那么就将当前事件的时间戳赋值给 currentTimestamp
    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {
        if (word.getTimestamp() > currentTimestamp) {
            this.currentTimestamp = word.getTimestamp();
        }
        return currentTimestamp;
    }

    @Nullable
    @Override
    //获取当前的水位线
    public Watermark getCurrentWatermark() {
        //最大允许数据延迟时间为 5s，超过 5s 的话如果还来了之前早的数据，那么 Flink 就会丢弃了，
        //因为 Flink 的窗口中的数据是要触发的，不可能一直在等着这些迟到的数据而不让窗口触发结束进行计算操作
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }
}
```

通过定义这个时间，可以避免部分数据因为网络或者其他的问题导致不能够及时上传从而不把这些事件数据作为计算的，那么如果在这延迟之后还有更早的数据到来的话，那么 Flink 就会丢弃了，所以合理的设置这个允许延迟的时间也是一门细活，得观察生产环境数据的采集到消息队列再到 Flink 整个流程是否会出现延迟，统计平均延迟大概会在什么范围内波动。这也就是说明了一个事实那就是 Flink 中设计这个水印的根本目的是来解决部分数据乱序或者数据延迟的问题，而不能真正做到彻底解决这个问题，不过这一特性在相比于其他的流处理框架已经算是非常给力了。

###### 2.3.1 AssignerWithPeriodicWatermarks 

这个接口有四个实现类，分别如下图：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-082804.png)

###### 2.3.2 BoundedOutOfOrdernessTimestampExtractor

该类用来发出滞后于数据时间的水印，它的目的其实就是和我们上面定义的那个类作用是类似的，你可以传入一个时间代表着可以允许数据延迟到来的时间是多长。该类内部实现如下：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-083043.png)

你可以像下面一样使用该类来分配时间和生成水印：

```java
//Time.seconds(10) 代表允许延迟的时间大小
dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(10)) {
    //重写 BoundedOutOfOrdernessTimestampExtractor 中的 extractTimestamp()抽象方法
    @Override
    public long extractTimestamp(Event event) {
        return event.getTimestamp();
    }
})
```

###### 2.3.3 CustomWatermarkExtractor

这是一个自定义的周期性生成水印的类，在这个类里面的数据是 KafkaEvent。

###### 2.3.4 AscendingTimestampExtractor

时间戳分配器和水印生成器，用于时间戳单调递增的数据流，如果数据流的时间戳不是单调递增，那么会有专门的处理方法，代码如下：

```java
public final long extractTimestamp(T element, long elementPrevTimestamp) {
    final long newTimestamp = extractAscendingTimestamp(element);
    if (newTimestamp >= this.currentTimestamp) {
        this.currentTimestamp = ne∏wTimestamp;
        return newTimestamp;
    } else {
        violationHandler.handleViolation(newTimestamp, this.currentTimestamp);
        return newTimestamp;
    }
}
```

- IngestionTimeExtractor：依赖于机器系统时间，它在 extractTimestamp 和 getCurrentWatermark 方法中是根据 `System.currentTimeMillis()` 来获取时间的，而不是根据事件的时间，如果这个时间分配器是在数据源进 Flink 后分配的，那么这个时间就和 Ingestion Time 一致了，所以命名也取的就是叫 IngestionTimeExtractor。

**注意**：

1、使用这种方式周期性生成水印的话，你可以通过 `env.getConfig().setAutoWatermarkInterval(...);` 来设置生成水印的间隔（每隔 n 毫秒）。

2、通常建议在数据源（source）之后就进行生成水印，或者做些简单操作比如 filter/map/flatMap 之后再生成水印，越早生成水印的效果会更好，也可以直接在数据源头就做生成水印。比如你可以在 source 源头类中的 run() 方法里面这样定义

```java
@Override
public void run(SourceContext<MyType> ctx) throws Exception {
    while (/* condition */) {
        MyType next = getNext();
        ctx.collectWithTimestamp(next, next.getEventTimestamp());

        if (next.hasWatermarkTime()) {
            ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
        }
    }
}
```

##### 2.4 每个 Kafka 分区的时间戳

当以 Kafka 来作为数据源的时候，通常每个 Kafka 分区的数据时间戳是递增的（事件是有序的），但是当你作业设置多个并行度的时候，Flink 去消费 Kafka 数据流是并行的，那么并行的去消费 Kafka 分区的数据就会导致打乱原每个分区的数据时间戳的顺序。在这种情况下，你可以使用 Flink 中的 `Kafka-partition-aware` 特性来生成水印，使用该特性后，水印会在 Kafka 消费端生成，然后每个 Kafka 分区和每个分区上的水印最后的合并方式和水印在数据流 shuffle 过程中的合并方式一致。

如果事件时间戳严格按照每个 Kafka 分区升序，则可以使用前面提到的 AscendingTimestampExtractor 水印生成器来为每个分区生成水印。下面代码教大家如何使用 `per-Kafka-partition` 来生成水印。

```java
FlinkKafkaConsumer011<Event> kafkaSource = new FlinkKafkaConsumer011<>("zhisheng", schema, props);
kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {

    @Override
    public long extractAscendingTimestamp(Event event) {
        return event.eventTimestamp();
    }
});

DataStream<Event> stream = env.addSource(kafkaSource);
```

下图表示水印在 Kafka 分区后如何通过流数据流传播：

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-09-014107.jpg)

##### 2.5 Watermark 与 Window 结合来处理延迟数据

其实在上文中已经提到的一点是在设置 Periodic Watermark 时，是允许提供一个参数，表示数据最大的延迟时间。其实这个值要结合自己的业务以及数据的情况来设置，如果该值设置的太小会导致数据因为网络或者其他的原因从而导致乱序或者延迟的数据太多，那么最后窗口触发的时候，可能窗口里面的数据量很少，那么这样计算的结果很可能误差会很大，对于有的场景（要求正确性比较高）是不太符合需求的。但是如果该值设置的太大，那么就会导致很多窗口一直在等待延迟的数据，从而一直不触发，这样首先就会导致数据的实时性降低，另外将这么多窗口的数据存在内存中，也会增加作业的内存消耗，从而可能会导致作业发生 OOM 的问题。

综上建议：

- 合理设置允许数据最大延迟时间
- 不太依赖事件时间的场景就不要设置时间策略为 EventTime

##### 2.6 延迟数据该如何处理(三种方法)

###### 2.6.1 丢弃（默认）

在 Flink 中，对这么延迟数据的默认处理方式是丢弃。

###### 2.6.2 allowedLateness 

再次指定允许数据延迟的时间，allowedLateness 表示允许数据延迟的时间，这个方法是在 WindowedStream 中的，用来设置允许窗口数据延迟的时间，超过这个时间的元素就会被丢弃，这个的默认值是 0，该设置仅针对于以事件时间开的窗口，它的源码如下：

```java
public WindowedStream<T, K, W> allowedLateness(Time lateness) {
    final long millis = lateness.toMilliseconds();
    checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

    this.allowedLateness = millis;
    return this;
}
```

之前有多个小伙伴问过我 Watermark 中允许的数据延迟和这个数据延迟的区别是啥？我的回复是该允许延迟的时间是在 Watermark 允许延迟的基础上增加的时间。那么具体该如何使用 allowedLateness 呢。

```java
dataStream.assignTimestampsAndWatermarks(new TestWatermarkAssigner())
    .keyBy(new TestKeySelector())
    .timeWindow(Time.milliseconds(1), Time.milliseconds(1))
    .allowedLateness(Time.milliseconds(2))  //表示允许再次延迟 2 毫秒
    .apply(new WindowFunction<Integer, String, Integer, TimeWindow>() {
        //计算逻辑
    });
```

###### 2.6.3 sideOutputLateData

sideOutputLateData 这个方法同样是 WindowedStream 中的方法，该方法会将延迟的数据发送到给定 OutputTag 的 side output 中去，然后你可以通过 `SingleOutputStreamOperator.getSideOutput(OutputTag)` 来获取这些延迟的数据。具体的操作方法如下：

```java
//定义 OutputTag
OutputTag<Integer> lateDataTag = new OutputTag<Integer>("late"){};

SingleOutputStreamOperator<String> windowOperator = dataStream
        .assignTimestampsAndWatermarks(new TestWatermarkAssigner())
        .keyBy(new TestKeySelector())
        .timeWindow(Time.milliseconds(1), Time.milliseconds(1))
        .allowedLateness(Time.milliseconds(2))
        .sideOutputLateData(lateDataTag)    //指定 OutputTag
        .apply(new WindowFunction<Integer, String, Integer, TimeWindow>() {
            //计算逻辑
        });

windowOperator.addSink(resultSink);

//通过指定的 OutputTag 从 Side Output 中获取到延迟的数据之后，你可以通过 addSink() 方法存储下来，这样可以方便你后面去排查哪些数据是延迟的。
windowOperator.getSideOutput(lateDataTag)
        .addSink(lateResultSink);
```

