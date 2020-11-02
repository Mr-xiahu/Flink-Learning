## Flink State详解

### 一.什么是State

~~~tex
 Flink 是一款有状态的流处理框架.那这里的状态是什么意思?
 我们拿WordCount案例来说明,该程序是判断相同的word,并且将相同的word进行+1.
 state就是存贮word的count值
~~~

#### 1.1 为什么需要State

~~~
对于流处理系统,数据是一条一条进行消费的,默认是没有一个Object去对数据消费进度进行记录.
此时,如果该程序因为各种原因挂了,我们是不知道这些数据消费到了哪里的.等下次程序启动的时候,程序不确定上次消费到了哪里,所以会出现两种情况:
1.重复消费已经消费过的数据
2.漏掉没有消费的数据
简单解释一下:程序挂的那一刻,程序消费到了15:00.等到程序启动的时候---->
1.它可能从12:00开始消费,在此期间,12:00-15:00之间的数据就是重复消费.
2.它可能从17:00开始消费,在此期间,15:00-17:00之间的数据就是没有消费.
Flink为了解决这个问题,就产生了State.
state 中存储着每条数据消费后的数据消费点,当 Job 重启后,就能够从 checkpoint中的 state 数据进行恢复.
ps:
checkpoint:暂且认为Flink自动将state做一个全局快照,保证Flink的容错性.
~~~

### 二.State 的种类

#### 2.1 Keyed state

1. Keyed State 总是和具体的 key 相关联，它只能在 KeyedStream 的 function 和 operator 上使用。
   你可以将 Keyed State 当作是 Operator State 的一种特例，但是它是被分区或分片的.
2. 每个 Keyed State 分区对应一个 key 的 Operator State，对于某个 key 在某个分区上有唯一的状态。
   类似于:一个topic在一个partition上对应着唯一的offset.
3. 逻辑上,因为每个具体的 key 总是属于唯一一个具体的 parallel-operator-instance（并行操作实例）,那么此时便可以将该Keyed State 看成Tuple2<Key,parallel-operator-instance>.
4. .Keyed State 可以进一步组织成 Key Group，Key Group 是 Flink 重新分配 Keyed State 的最小单元，所以有多少个并行，就会有多少个 Key Group。
5. 在执行过程中，每个 keyed operator 的并行实例会处理来自不同 key 的不同 Key Group。

#### 2.2 Operator state

1. 每个 operator state 都对应着一个并行实例。比如：每个 Kafka consumer 的并行实例都会持有一份topic partition 和 offset 的 map，这个 map 就是它的 Operator State。
2. 当并行度发生变化时，Operator State 可以将状态重分配于所有的并行实例中，并且提供了多种方式来进行重分配

### 三.Raw and Managed State

Keyed State 和 Operator State 都有两种存在形式，即 Raw State（原生状态）和 Managed State（托管状态）

#### 3.1  Raw State（原生状态）

原始状态是 Operator（算子）保存它们自己的数据结构中的 state，当 checkpoint 时，原始状态会以字节流的形式写入进 checkpoint 中。Flink 并不知道 State 的数据结构长啥样，仅能看到原生的字节数组

#### 3.2 Managed State（托管状态）

托管状态可以使用 Flink runtime 提供的数据结构来表示，例如内部哈希表或者 RocksDB。具体有 ValueState，ListState 等。Flink runtime 会对这些状态进行编码然后将它们写入到 checkpoint 中。

#### 3.3 总结

1. DataStream 的所有 function 都可以使用托管状态，但是原生状态只能在实现 operator 的时候使用。
2. 相对于原生状态，推荐使用托管状态，因为如果使用托管状态，当并行度发生改变时，Flink 可以自动的帮你重分配 state，同时还可以更好的管理内存。
3. Flink目前不支持托管状态做特殊的序列化

### 四.如何使用托管 Keyed State

托管的 Keyed State 接口提供对不同类型状态（元素的 key获取）的访问，这意味着这种状态只能在通过 stream.keyBy() 创建的 KeyedStream 上使用。

~~~java
//保存一个可以更新和获取的值,可以用 update(T) 来更新 value，可以用 value() 来获取 value
ValueState:
//保存一个值的列表，用 add(T) 或者 addAll(List) 来添加，用 Iterable get() 来获取。
ListState
//保存一个值,这个值是状态的很多值的聚合结果，接口和 ListState 类似，但是可以用相应的 ReduceFunction 来聚合
ReducingState
//保存很多值的聚合结果的单一值，与 ReducingState 相比，聚合类型可以和元素类型不同，提供 AggregateFunction 来实现聚合
AggregatingState
//保存一组映射，可以将 kv 放进这个状态，使用 put(UK, UV) 或者 putAll(Map) 添加，或者使用 get(UK) 获取。
MapState
//与 AggregatingState 类似，不能使用 FoldFunction 进行聚合。
FoldingState   不建议使用

    
//所有类型的状态都有一个 clear() 方法来清除当前的状态。
~~~

要使用一个状态对象，需要先创建一个 StateDescriptor，它包含了状态的名字，状态的值的类型，或许还有一个用户定义的函数，比如 ReduceFunction。根据你想要使用的 state 类型，你可以创建ValueStateDescriptor、ListStateDescriptor、ReducingStateDescriptor、FoldingStateDescriptor 或者 MapStateDescriptor

下面时ValueState的使用:

~~~java
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    //ValueState 使用方式，第一个字段是 count,第二个字段是运行的和
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> in, Collector<Tuple2<Long, Long>> collector) throws Exception {
        //访问状态的 value 值
        Tuple2<Long, Long> currentSum = sum.value();//A
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

    
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", //状态名称
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }), //状态类型信息
                        Tuple2.of(0L, 0L)); //状态的默认值
        sum = super.getRuntimeContext().getState(descriptor);//获取状态
    }
    
        public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 4L),
                Tuple2.of(1L, 2L)
        )
                    .keyBy(0)
                    .flatMap(new CountWindowAverage())
                    .print();

        env.execute();
    }
}
~~~

解释:

~~~
数据源数据如下:
	Tuple2.of(1L, 3L)
   	Tuple2.of(1L, 5L)
   	Tuple2.of(1L, 7L)
   	Tuple2.of(1L, 4L)
   	Tuple2.of(1L, 2L)
1.Tuple2.of(1L, 3L)第一个进来.首先执行open()方法.得到的currentSum = （0,0）
2.开始执行flatmap(),经过A，B步骤后,currentSum=（1,3）.执行C步骤,更新ValueState值,也就是更新状态值.目前,状态值	为:(1,3)
3.第二个进来的是:Tuple2.of(1L, 5L),首先执行open()方法,获取ValueState的状态值:(1,3).
	紧跟着经过:A,B两步.currentSum=(2,8).并且ValueState.value的值更新为:(2,8),
	因为f0 >= 2,所以执行:E,结束后;currentSum=（1，4）,并且输出.清除sum，也就是清空状态.
4.第三个进来的是:Tuple2.of(1L, 7L)，首先执行open()方法.因为3.中,状态被清除了,所以得到的currentSum = （0,0）
5.开始执行flatmap(),经过A，B步骤后,currentSum=（1,3）.执行C步骤,更新ValueState值,也就是更新状态值.目前,状态值	为:(1,7)
6.第四个进来的是:Tuple2.of(1L, 4L),首先执行open()方法,获取ValueState的状态值:(1,7).
	紧跟着经过:A,B两步.currentSum=(2,11),并且ValueState.value的值更新为:(2,11),
	因为f0 >= 2,所以执行:E,结束后;currentSum=（1，5）,并且输出.清除sum，也就是清空状态.
........
~~~

### 五.如何使用托管 Operator State

实现下面两个接口:

1. CheckpointedFunction 
2. ListCheckpointed

#### 5.1 CheckpointedFunction 

~~~java
/**
 * 使用托管的Operator State之：实现CheckpointedFunction接口
 */
public class MyCheckpointedFunction implements CheckpointedFunction {

    //当请求 checkpoint 快照时,将调用此方法
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        /**
         * 当有请求执行 checkpoint 的时候,napshotState() 方法就会被调用
         */
    }

    //在分布式执行期间创建并行功能实例时,将调用此方法.函数通常在此方法中设置其状态存储数据结构
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        /**
         * initializeState() 方法会在每次初始化用户定义的函数时,或者从更早的 checkpoint 恢复的时候被调用.
         * 因此 initializeState() 不仅是不同类型的状态被初始化的地方,而且还是 state 恢复逻辑的地方.
         */
    }
}
~~~

###### List 类型的托管

1. Even-split redistribution

   每个算子会返回一个状态元素列表，整个状态在逻辑上是所有列表的连接。在重新分配或者恢复的时候，这个状态元素列表会被按照并行度分为子列表，每个算子会得到一个子列表。

   这个子列表可能为空，或包含一个或多个元素。举个例子，如果使用并行性 1，算子的检查点状态包含元素 element1 和 element2，当将并行性增加到 2 时，element1 可能最终在算子实例 0 中，而 element2 将转到算子实例 1 中

2. Union redistribution

   每个算子会返回一个状态元素列表，整个状态在逻辑上是所有列表的连接。在重新分配或恢复的时候，每个算子都会获得完整的状态元素列表。

下面是一个有状态的 SinkFunction 的示例，它使用 CheckpointedFunction 来缓存数据，然后再将这些数据发送到外部系统，使用了 Even-split 策略

~~~java
/**
 * 一个有状态的 SinkFunction 的示例，
 * 它使用 CheckpointedFunction 来缓存数据，
 * 然后再将这些数据发送到外部系统，使用了 Even-split 策略：
 */
public class MyCheckpointedFunctionImpl implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public MyCheckpointedFunctionImpl(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                //将数据发到外部系统
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
         /**
         * initializeState 方法将 FunctionInitializationContext 作为参数，它用来初始化 non-keyed 状态。
         * 注意状态是如何初始化的，类似于 Keyed state，StateDescriptor 包含状态名称和有关状态值的类型的信息
         */
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
~~~

#### 5.2 ListCheckpointed

是一种受限的 CheckpointedFunction，只支持 List 风格的状态和 even-spit 的重分配策略

~~~java
/**
 * 使用托管的Operator State之：实现ListCheckpointed接口
 */
public class MyListCheckpointedFunction implements ListCheckpointed {


    /**
     * 获取函数的当前状态。状态必须返回此函数先前所有的调用结果。
     */
    @Override
    public List snapshotState(long l, long l1) throws Exception {
        return null;
    }

    /**
     * 将函数或算子的状态恢复到先前 checkpoint 的状态.
     * 此方法在故障恢复后执行函数时调用.
     * 如果函数的特定并行实例无法恢复到任何状态，则状态列表可能为空.
     */
    @Override
    public void restoreState(List list) throws Exception {

    }
}
~~~

### 六.Stateful Source Functions

```java
/**
 * 与其他算子相比，有状态的 source 函数需要注意的地方更多
 */
public class MyListCheckpointedFunctionImpl extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {

    //一次语义的当前偏移量
    private Long offset = 0L;

    //作业取消标志
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) {
        final Object lock = ctx.getCheckpointLock();
        /**
         * 为了保证状态的更新和结果的输出原子性,用户必须在 source 的 context 上加锁.
         */
        while (isRunning) {
            //输出和状态更新是原子性的
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) {
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> state) {
        for (Long s : state)
            offset = s;
    }
}
```

### 七.何时结束Checkpoint

~~~java
/**
 * 有些算子想知道什么时候 checkpoint 全部做完了，
 * 可以参考使用 org.apache.flink.runtime.state.CheckpointListener 接口来实现，
 * 在该接口里面有 notifyCheckpointComplete 方法。
 */
public class MyCheckpointListener implements CheckpointListener {

    /**
     *
     * @param id   The ID of the checkpoint that has been completed.
     * @throws Exception
     */
    @Override
    public void notifyCheckpointComplete(long id) throws Exception {
        //返回CheckPoint的结束时间戳
    }
}
~~~

### 八. State TTL(存活时间)

#### 8.1 State TTL 介绍

TTL 可以分配给任何类型的 Keyed state.

如果一个状态设置了 TTL，那么当状态过期时，那么之前存储的状态值会被清除。所有的状态集合类型都支持单个入口的 TTL，这意味着 List 集合元素和 Map 集合都支持独立到期。

为了使用状态 TTL，首先必须要构建 StateTtlConfig 配置对象，然后可以通过传递配置在 State descriptor 中启用 TTL 功能：

#### 8.2 构建最简单的TTL

~~~java
/**
 * 构建StateTTL
 */
public class StateTtlDemo extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("xiahu", String.class);

        //构建StateTtlConfig
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))//状态存活时间
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//哪种操作会使状态更新
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//是否在读取访问时返回过期值
                .build();
        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
~~~

~~~
UpdateType 配置状态 TTL:默认为 OnCreateAndWrite
1.StateTtlConfig.UpdateType.OnCreateAndWrite: 仅限创建和写入访问时更新
2.StateTtlConfig.UpdateType.OnReadAndWrite: 除了创建和写入访问，还支持在读取时更新

StateVisibility 配置是否在读取访问时返回过期值（如果尚未清除），默认为NeverReturnExpired
1.StateTtlConfig.StateVisibility.NeverReturnExpired: 永远不会返回过期值
2.StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp: 如果仍然可用则返回
~~~

注意：

- 状态后端会存储上次修改的时间戳以及对应的值，启用此功能会增加状态存储的消耗，堆状态后端存储一个额外的 Java 对象，其中包含对用户状态对象的引用和内存中原始的 long 值。RocksDB 状态后端存储为每个存储值、List、Map 都添加 8 个字节。
- 目前仅支持参考 processing time 的 TTL
- 使用启用 TTL 的描述符去尝试恢复先前未使用 TTL 配置的状态可能会导致兼容性失败或者 StateMigrationException 异常。
- TTL 配置并不是 Checkpoint 和 Savepoint 的一部分，而是 Flink 如何在当前运行的 Job 中处理它的方式。
- 只有当用户值序列化器可以处理 null 值时，具体 TTL 的 Map 状态当前才支持 null 值，如果序列化器不支持 null 值，则可以使用 NullableSerializer 来包装它（代价是需要一个额外的字节）。

#### 8.3 清除过期的state

~~~java
/**
 * 清除过期State
 */
public class StateTtlDemo2 extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("xiahu", String.class);

        /**
         * 默认情况下,过期值只有在显式读出时才会被删除,例如通过调用 ValueState.value().
         * 这意味着只要不 显示读出,state会越来愈大.
         * 你可以在进行sum.value()时获取完整的状态,然后执行sum.clean()去清理过期的状态
         */


        //构建StateTtlConfig
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .cleanupFullSnapshot()//清除过去快照(状态)
                .build();

        /**
         * 此配置不适用于 RocksDB 状态后端中的增量 checkpoint.
         * 对于现有的 Job,可以在 StateTtlConfig 中随时激活或停用此清理策略,例如,从保存点重启后。
         */

        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
~~~

#### 8.4 后台激活清理

~~~java
/**
 * 后台激活清理State
 */
public class StateTtlDemo3 extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("xiahu", String.class);

        /**
         * 如果使用的后端支持以下选项,则会激活 StateTtlConfig 中的默认后台清理.
         */

        //构建StateTtlConfig
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .cleanupInBackground()
                .build();

        /**
         * 要在后台对某些特殊清理进行更精细的控制,可以按照下面的说明单独配置它.
         * 目前,堆状态后端依赖于增量清理,RocksDB 后端使用压缩过滤器进行后台清理
         */
        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
~~~





