## Flink是如何实现Exactly Once ?



在了解Exactly Once 之前，首先得先知道Flink内部的Checkpoint 是干啥的？



### 1. CheckPoint 简介

Flink Checkpoint机制的存在，是为了解决Flink任务在运行的过程中，由于各种问题导致任务异常中断后，能正常恢复。我们看看在Checkpoint 的过程中，到底做了哪些事？



Checkpoint 是通过**快照**的方式，将程序在某个时刻的状态，通过快照保存下来，当程序发送错误时，默认去最近一次保存的快照中恢复。（快照暂且将其理解成为照片）



通俗举例说明：

目前我需要统计上海市内，每个区的人数分别为多少？（假设我现在拥有某个接口，获取上海市所有人的上海居住地，并且这些数据存入topic: checkpoint_test 内）;

~~~
1.使用FlinkKafkaConsumer 消费checkpoint_test 的数据，根据居住地的不同，将起分别存入Map:
	假设目前topic内暂时只有两个区的人数统计
	<Area,Count> 
	<松江,2000>
	<徐汇,3000>
	此时topic 内的offset 为<checkpoint_test,5000>，
2.我们设置的checkpoint间隔时间为5分钟。在这5分钟内，flink又消费了一批数据：<松江,5000>,<徐汇,5000>，此时topic 内的offset 为<checkpoint_test,10000>，恰好到了第一次checkpoint的时候，Flink将下面内容存入State,保存到checkpoint中
	checkpoint_1:
		topic：<checkpoint_test,10000>
		data:<松江,5000>,<徐汇,5000>
		
3.又过了5分钟后，flink又消费了一批数据：<松江,10000>,<徐汇,10000>，此时topic 内的offset 为<checkpoint_test,20000>，恰好到了第二次checkpoint的时候，Flink将下面内容存入State,保存到checkpoint中
	checkpoint_2:
		topic：<checkpoint_test,20000>
		data:<松江,10000>,<徐汇,10000>
		
4.又过了2分钟，目前数据统计如下：<松江,10500>,<徐汇,10600>，此时topic 内的offset 为<checkpoint_test,21100>，正在此时，Flink程序由于出现一些异常，导致程序挂了，Flink JobManager会尝试自动从checkpoint重启程序。
你想想，Flink 会去读取哪个Checkpoint的内容并完成重启呢？
答案是：checkpoint_2:
		topic：<checkpoint_test,20000>
		data:<松江,10000>,<徐汇,10000>
虽然在第三个5分钟内，Flink消费topic offset 如下：<checkpoint_test,21100>，并且数据统计如下:<松江,10500>,<徐汇,10600>,但是最终这些数据并没有做checkpoint，它们并没有被保存到State内部.
~~~



### 2. Barrier

![](https://img-blog.csdnimg.cn/2021030817491552.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDg2NTU3NA==,size_16,color_FFFFFF,t_70#pic_center)



~~~
1.barrier从sourceTask,一直流向sinkTask,期间只要有Task(MapTask,ReduceTask等)碰到barrier，便会触发snapshotState()注意，需要实现CheckpointFuncation接口.checkpoint barrier 4 处做的快照就是指从 Job 开始到处理到 barrier 4 所有的状态数据。(data:1-12)
2.对应到：统计上海市内，每个区的人数案例中则是:
	a.当sourceTask进行第一次checkpoint时，sourceTask需要进行快照，正好将：<checkpoint_test,0>信息写入State,并向数据流中插入checkpoint barrier 1,将<checkpoint_test,0>保存到State
	b.当sourceTask进行第二次checkpoint时，sourceTask需要进行快照，发现自己恰好收到offset<checkpoint_test,4>处的数据，所以会向数据流中插入checkpoint barrier 2，该checkpoint barrier具体位置为:<checkpoint_test,4>与<checkpoint_test,5>的中间，对应到图上便是:data4 与 data5之间；并且将<checkpoint_test,4>保存到State；sourceTask 会将checkpoint barrier 2 和 数据(data 1,2,3,4)一起发给后面。当其他Task(MapTask)遇上checkpoint barrier 2 时，该Task会先将checkpoint barrier 2之前的数据(data:1,2,3,4)统计好，先保存到State中，再来处理先将checkpoint barrier 2之后的数据.
	c:当sourceTask进行第三次checkpoint时，sourceTask需要进行快照，发现自己恰好收到offset<checkpoint_test,8>处的数据，所以会向数据流中插入checkpoint barrier 3，该checkpoint barrier具体位置为:<checkpoint_test,8>与<checkpoint_test,9>的中间，对应到图上便是:data8 与 data9之间；并且将<checkpoint_test,8>保存到State；sourceTask 会将checkpoint barrier 3 和 数据(data 5,6,7,8)一起发给后面。当其他Task(MapTask)遇上checkpoint barrier 3 时，该Task会先将checkpoint barrier 3之前的数据(data:5,6,7,8)统计好，先保存到State中，再来处理先将checkpoint barrier 3之后的数据.
~~~



### 3. 多并行度下的Checkpoint



![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151553.jpg)

整个 Checkpoint 的过程跟之前单并行度类似，图中有 4 个带状态的 Operator 实例，相应的状态后端就可以想象成 4 个格子。整个 Checkpoint 的过程可以当做 Operator 实例填自己格子的过程，Operator 实例将自身的状态写到状态后端中相应的格子，当所有的格子填满可以简单地认为一次完整的 Checkpoint 做完了。

上面只是快照的过程，Checkpoint 执行过程如下：

1. JobManager 端的 CheckPointCoordinator 向所有 Source Task 发送 CheckPointTrigger，Source Task 会在数据流中安插 Checkpoint barrier。

2. 当 task 收到所有的 barrier 后，向自己的下游继续传递 barrier，然后自身执行快照，并将自己的状态**异步写入到持久化存储**中。
   - 增量 CheckPoint 只是把最新的一部分数据更新写入到外部存储；
   - 为了下游尽快开始做 CheckPoint，所以会先发送 barrier 到下游，自身再同步进行快照。

3. 当 task 对状态的快照信息完成备份后，会将备份数据的地址（state handle）通知给 JobManager 的 CheckPointCoordinator。
   - 如果checkpoint 超时，CheckPointCoordinator 还没有收集完所有的 State Handle，CheckPointCoordinator会把这次 Checkpoint 产生的所有状态数据全部删除。

4. CheckPointCoordinator 把整个 StateHandle 封装成 completed Checkpoint Meta，写入到 HDFS，整个 Checkpoint 结束。



### 4. Barrier 对齐

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151558.jpg)

如上图所示，当前的 Operator 实例接收上游两个流的数据，一个是字母流，一个是数字流。

无论是字母流还是数字流，都会往流中发送 Checkpoint barrier。但是由于算子处理数据的速度不一样，flink在这种情况下是如何判断何时进行checkpoint的呢？

答案是：`当一个 Operator 实例有多个输入流时，Operator 实例需要在做快照之前进行 barrier 对齐，等待所有输入流的 barrier 都到达`

barrier 对齐的详细过程如下所示：

~~~
1.对于一个有多个输入流的 Operator 实例，当 Operator 实例从其中一个输入流接收到 Checkpoint barrier n 时，就不能处理来自该流的任何数据记录了，直到它从其他所有输入流接收到 barrier n 为止；如上图中第 1 个小图所示，数字流的 barrier 先到达了，便不会继续接受数字流的数据，等到字母流的barrier到了之后再做打算；

2.首先接收到 barrier n 的流暂时被搁置，从这些流接收的数据不会被立即处理，而是放入输入缓冲区。图 2 中，我们可以看到虽然数字流对应的 barrier 已经到达了，但是 barrier 之后的 ‘1、2、3’ 这些数据只能放到缓冲区中，等待字母流的 barrier 到达；

3.一旦最后所有输入流都接收到 barrier n，Operator 实例就会把 barrier 之前所有已经处理完成的数据和 barrier n 一块发送给下游。然后 Operator 实例就可以对状态信息进行快照。如图 3 所示，Operator 实例接收到上游所有流的 barrier n，此时 Operator 实例就可以将 barrier 和 barrier 之前的数据发送到下游，然后自身状态进行快照；

4.快照做完后，Operator 实例将继续处理缓冲区的记录，然后就可以处理输入流的数据。如图 4 所示，先处理完缓冲区数据，就可以正常处理输入流的数据了；
~~~

如果 barrier 不对齐会发生什么样的情况呢？

~~~
barrier 不对齐是指当还有其他流的 barrier 还没到达时，为了提高 Operator 实例的处理性能，Operator 实例会直接处理 barrier 之后的数据，等到所有流的barrier 都到达后，就可以对该 Operator 做 Checkpoint 快照了；
barrier 不对齐时会直接把 barrier 之后的数据 1、2、3 直接处理掉，而不是放到缓冲区中等待其他的输入流的 barrier 到达，当所有输入流的 barrier 都到达后，才开始对 Operator 实例的状态信息进行快照，这样会导致做快照之前，Operator 实例已经处理了一些 barrier n 之后的数据；

Checkpoint 的目的是为了保存快照信息，如果 barrier 不对齐，那么 Operator 实例在做第 n 次 Checkpoint 之前，已经处理了一些 barrier n 之后的数据，当程序从第 n 次 Checkpoint 恢复任务时，程序会从第 n 次 Checkpoint 保存的 offset 位置开始消费数据，就会导致一些数据被处理了两次，就出现了重复消费。如果进行 barrier 对齐，就不会出现这种重复消费的问题，所以，barrier 对齐就可以实现 Exactly Once，barrier 不对齐就变成了 At Least Once。
~~~

结合真实场景如下：

`目前存在topic: mulit_barrier, 两个 partition用来模拟多条流数据.`

1. barrier不对齐

   ![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151554.jpg)

   ~~~
   如左图所示，Source Subtask 0 和 Subtask 1 已经完成了快照操作，它们的状态信息为: 
   	offset(0,10000)
   	offset(1,10005)
   当 Source Subtask 1 的 barrier 到达 PV task 时，PV 结果为 20002，但 PV task 还没有接收到 Source Subtask 0 发送的 barrier，所以 PV task 还不能对自身状态信息进行快照。由于设置的 barrier 不对齐，所以此时 PV task 会继续处理 Source Subtask 0 和 Source Subtask 1 传来的数据。
   
   很快，如右图所示，PV task 接收到 Source Subtask 0 发来的 barrier，但是 PV task 已经处理了 Source Subtask 1 barrier 之后的三条数据，PV 值目前已经为 20008 了，这里的 PV=20008 实际上已经处理到 partition 1 offset 为 10008 的位置，此时 PV task 会对自身的状态信息（PV = 20008）做快照，整体的快照信息为 
   	offset(0,10000)
   	offset(1,10005) 
   	PV=20008。
   
   接着程序在继续运行，过了 10 秒，由于某个服务器故障，导致我们的 Operator 实例有一个挂了，所以 Flink 会从最近一次 Checkpoint 保存的状态恢复。那具体是怎么恢复的呢？
   
   Flink 同样会起三个 Operator 实例，我还称它们是 
   	Source Subtask 0、
   	Source Subtask 1  
   	PV task。
   三个 Operator 会从状态后端读取保存的状态信息。
   Source Subtask 0 会从 partition 0 offset 为 10000 的位置开始消费，
   Source Subtask 1 会从 partition 1 offset 为 10005 的位置开始消费，
   PV task 会基于 PV=20008 进行累加统计。
   然后就会发现的 PV 值 20008 实际上已经包含了 partition 1 的offset 10005~10008 的数据，
   所以 partition 1 从 offset 10005 恢复任务时，partition1 的 offset 10005~10008 的数据被消费了两次，出现了重复消费的问题，所以 barrier 不对齐只能保证 At Least Once。
   ~~~

2. barrier对齐

   ![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151555.jpg)

   ~~~
   如上图图所示，当 PV task 接收到 Source Subtask 1 的 barrier 后，并不会处理 Source Subtask 1 barrier 之后的数据，而是把这些数据放到 PV task 的输入缓冲区中，直到等到 Source Subtask 0 的 barrier 到达后，PV task 才会对自身状态信息进行快照。
   
   此时 PV task 会把 PV=20005 保存到快照信息中，整体的快照状态信息为 
   	offset(0,10000)
   	offset(1,10005) 
   	PV=20005，
   当任务从 Checkpoint 恢复时，
   Source Subtask 0 会从 partition 0 offset 为 10000 的位置开始消费，
   Source Subtask 1 会从 partition 1 offset 为 10005 的位置开始消费，
   PV task 会基于 PV=20005 进行累加统计，
   所以 barrier 对齐能保证 Flink 内部的 Exactly Once。
   
   在 Flink 应用程序中，当 Checkpoint 语义设置 Exactly Once 或 At Least Once 时，唯一的区别就是 barrier 对不对齐。当设置为 Exactly Once 时，就会 barrier 对齐，当设置为 At Least Once 时，就会 barrier 不对齐。
   ~~~



解释：

~~~
barrier 对齐其实是要付出代价的，从 barrier 对齐的过程可以看出，PV task 明明可以更高效的处理数据，但因为 barrier 对齐，导致 Source Subtask 1 barrier 之后的数据被放到缓冲区中，暂时性地没有被处理，假如生产环境中，Source Subtask 0 的 barrier 迟迟没有到达，比 Source Subtask 1 延迟了 30 秒，那么这 30 秒期间，Source Subtask 1 barrier 之后的数据不能被处理，所以 PV task 相当于被闲置了。

1.我们的一些业务场景对 Exactly Once 要求不高时，我们可以设置 Flink 的 Checkpoint 语义是 At Least Once 来小幅度的提高应用程序的执行效率。
Flink Web UI 的 Checkpoint 选项卡中可以看到 barrier 对齐的耗时，如果发现耗时比较长，且对 Exactly Once 语义要求不高时，可以考虑使用该优化方案。

如何在不中断运算的前提下产生快照？在 Flink 的 Checkpoint 过程中，无论下游算子有没有做完快照，只要上游算子将 barrier 发送到下游且上游算子自身已经做完快照时，那么上游算子就可以处理 barrier 之后的数据了，从而使得整个系统 Checkpoint 的过程影响面尽量缩到最小，来提升系统整体的吞吐量。

checkpoint 时间间隔需要结合业务场景设定；


有的同学可能还有疑问，明明说好的 Exactly Once，但在 Checkpoint 成功后 10s 发生了故障，从最近一次成功的 Checkpoint 处恢复时，由于发生故障前的 10s Flink 也在处理数据，所以 Flink 应用程序肯定是把一些数据重复处理了呀。

在面对任意故障时，不可能保证每个算子中用户定义的逻辑在每个事件中只执行一次，因为用户代码被部分执行的可能性是永远存在的。那么，当引擎声明 Exactly Once 处理语义时，它们能保证什么呢？如果不能保证用户逻辑只执行一次，那么哪些逻辑只执行一次？当引擎声明 Exactly Once 处理语义时，它们实际上是在说，它们可以保证引擎管理的状态更新只提交一次到持久的后端存储。换言之，无论以什么维度计算 PV、无论 Flink 应用程序发生多少次故障导致重启从 Checkpoint 恢复，Flink 都可以保证 PV 结果是准确的，不会因为各种任务重启而导致 PV 值计算偏高。

为了下游尽快做 Checkpoint，所以会先发送 barrier 到下游，自身再同步进行快照。这一步，如果向下发送 barrier 后，自己同步快照慢怎么办？下游已经同步好了，自己还没？可能会出现下游比上游快照还早的情况，但是这不影响快照结果，只是下游做快照更及时了，我只要保证下游把 barrier 之前的数据都处理了，并且不处理 barrier 之后的数据，然后做快照，那么下游也同样支持 Exactly Once。

失败的 Checkpoint 是不能用来恢复任务的，必须所有的算子的 Checkpoint 都成功，那么这次 Checkpoint 才能认为是成功的，才能用来恢复任务。
~~~



### 5. 端对端如何保证 Exactly Once



#### 5.1 幂等性写入

~~~
之前 Strom 或 Spark Streaming 的方案中，将统计的 PV 结果保存在 Redis 中，每来一条数据，从 Redis 中获取相应 app 对应的 PV 值然后内存中进行 +1 后，再将 PV 值 put 到 Redis 中。

例如：Redis 中保存 app1 的 PV 为 10，现在来了一条 app1 的日志，首先从 Redis 中获取 app1 的 PV 值 =10，内存中 10+1=11，将 (app1,11) put 到 Redis 中，这里的 11 就是我们统计的 app1 的 PV 结果。

当然 Flink 也可以用上述的这种方案来统计各 app 的 PV，但是上述方案并不能保证 Exactly Once，为什么呢？
当第 1 次 Checkpoint 时，app1 的 PV 结果为 10000，
第 1 次 Checkpoint 结束后运行了 10 秒，Redis 中 app1 的 PV 结果已经累加到了 10200。
此时如果任务挂了，从第 1 次 Checkpoint 恢复任务时，会继续按照 Redis 中保存的 PV=10200 进行累加，
但是正确的结果应该是从 PV=10000 开始累加。

Flink 内部状态可以保证 Exactly Once，这里可以将统计的 PV 结果保存在 Flink 内部的状态里，每次基于状态进行累加操作，并将累加到的结果 put 到 Redis 中，这样当任务从 Checkpoint 处恢复时，并不是基于 Redis 中实时统计的 PV 值进行累加，而是基于 Checkpoint 中保存的 PV 值进行累加，Checkpoint 中会保存每次 Checkpoint 时对应的 PV 快照信息；
例如：第 n 次 Checkpoint 会把当时 pv=10000 保存到快照信息里，同时状态后端还保存着一份实时的状态信息用于实时累加。
~~~

示例代码如下所示：

~~~java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 1 分钟一次 Checkpoint
env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));

CheckpointConfig checkpointConf = env.getCheckpointConfig();
// Checkpoint 语义 EXACTLY ONCE
checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer011<>(
        // kafka topic， String 序列化
        "app-topic",  new SimpleStringSchema(), props));

// 按照 appId 进行 keyBy
appInfoSource.keyBy((KeySelector<String, String>) appId -> appId)
        .map(new RichMapFunction<String, Tuple2<String, Long>>() {
            private ValueState<Long> pvState;
            private long pv = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化状态
                pvState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("pvStat",
                        TypeInformation.of(new TypeHint<Long>() {})));
            }

            @Override
            public Tuple2<String, Long> map(String appId) throws Exception {
                // 从状态中获取该 app 的 PV 值，+1 后，update 到状态中
                if(null == pvState.value()){
                    pv = 1;
                } else {
                    pv = pvState.value();
                    pv += 1;
                }
                pvState.update(pv);
                return new Tuple2<>(appId, pv);
            }
        })
        .print();

env.execute("Flink PV stat");
~~~

~~~
代码中设置 1 分钟一次 Checkpoint，
Checkpoint 语义 EXACTLY ONCE，
从 Kafka 中读取数据，这里为了简化代码，所以 Kafka 中读取的直接就是 String 类型的 appId，按照 appId KeyBy 后，执行 RichMapFunction，
RichMapFunction 的 open 方法中会初始化 ValueState<Long> 类型的 pvState，
pvState 就是上文一直强调的状态信息，每次 Checkpoint 的时候，会把 pvState 的状态信息快照一份到 HDFS 来提供恢复。

这里按照 appId 进行 keyBy，所以每一个 appId 都会对应一个 pvState，pvState 里存储着该 appId 对应的 pv 值。每来一条数据都会执行一次 map 方法，当这条数据对应的 appId 是新 app 时，pvState 里就没有存储这个 appId 当前的 pv 值，将 pv 值赋值为 1，当 pvState 里存储的 value 不为 null 时，拿出 pv 值 +1后 update 到 pvState 里。map 方法再将 appId 和 pv 值发送到下游算子，下游直接调用了 print 进行输出，这里完全可以替换成相应的 RedisSink 或 HBaseSink。

本案例中计算 pv 的工作交给了 Flink 内部的 ValueState，不依赖外部存储介质进行累加，外部介质承担的角色仅仅是提供数据给业务方查询，所以无论下游使用什么形式的 Sink，只要 Sink 端能够按照主键去重，该统计方案就可以保证 Exactly Once
~~~



#### 5.2 TwoPhaseCommitSinkFunction 

##### 1. 2PC 分布式一致性协议

~~~
在分布式系统中，每一个机器节点虽然都能明确地知道自己在进行事务操作过程中的结果是成功或失败，但无法直接获取到其他分布式节点的操作结果。因此，当一个事务操作需要跨越多个分布式节点的时候，为了让每个节点都能够获取到其他节点的事务执行状况，需要引入一个“协调者（Coordinator）”节点来统一调度所有分布式节点的执行逻辑，这些被调度的分布式节点被称为“参与者（Participant）”。协调者负责调度参与者的行为，并最终决定这些参与者是否要把事务真正的提交。

普通的事务可以保证单个事务内所有操作要么全部成功，要么全部失败;
分布式系统中如何保证多台节点上执行的事务要么全部成功，要么全部失败呢？
先了解一下 2PC 一致性协议。

2PC 是 Two-Phase Commit 的缩写，即两阶段提交。2PC 将分布式事务分为了两个阶段，分别是提交事务请求（投票）和执行事务提交。协调者会根据参与者在第一阶段的投票结果，来决定第二阶段是否真正的执行事务，具体流程如下。
~~~

###### 1.  提交事务请求

1. 协调者向所有参与者发送 prepare 请求与事务内容，询问是否可以准备事务提交，并等待参与者的响应。
2. 各参与者执行事务操作，并记录 Undo 日志（回滚）和 Redo日志（重放），但不真正提交。
3. 参与者向协调者返回事务操作的执行结果，执行成功返回 Yes，否则返回 No。



###### 2. 执行事务提交

事务请求成功：

1. 协调者向所有参与者发送 Commit 请求。
2. 参与者收到 Commit 请求后，会正式执行事务提交操作，并在提交完成后释放事务资源。
3. 完成事务提交后，向协调者发送 Ack 消息。
4. 协调者收到所有参与者的 Ack 消息，完成事务。
5. 参与者收到 Commit 请求后，将事务真正地提交上去，并释放占用的事务资源，并向协调者返回 Ack。
6. 协调者收到所有参与者的 Ack 消息，事务成功完成。

事物请求失败：

1. 协调者向所有参与者发送 Rollback 请求。
2. 参与者收到 Rollback 请求后，根据 Undo 日志回滚到事务执行前的状态，释放占用的事务资源。
3. 参与者在完成事务回滚后，向协调者返回 Ack。
4. 协调者收到所有参与者的 Ack 消息，事务回滚完成。



##### 2. 2PC  优缺点

2PC 的优点：原理简单，实现方便。

2PC 的缺点：

1. 协调者单点问题：协调者在整个 2PC 协议中非常重要，一旦协调者故障，则 2PC 将无法运转。

2. 过于保守：在 2PC 的阶段一，如果参与者出现故障而导致协调者无法获取到参与者的响应信息，这时协调者只能依靠自身的超时机制来判断是否需要中断事务，这种策略比较保守。换言之，2PC 没有涉及较为完善的容错机制，任意一个节点失败都会导致整个事务的失败。

3. 同步阻塞：执行过程是完全同步的，各个参与者在等待其他参与者投票响应的的过程中，将无法进行其他任何操作。

4. 数据不一致：在二阶段提交协议的阶段二，当协调者向所有的参与者发送 Commit 请求后，出现了局部网络异常或局部参与者机器故障等因素导致一部分的参与者执行了 Commit 操作，而发生故障的参与者没有执行 Commit，于是整个分布式系统便出现了数据不一致现象。



##### 3. TwoPhaseCommitSinkFunction

定义了如下 5 个抽象方法：

~~~java
// 处理每一条数据
protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;
// 开始一个事务，返回事务信息的句柄
protected abstract TXN beginTransaction() throws Exception;
// 预提交（即提交请求）阶段的逻辑
protected abstract void preCommit(TXN transaction) throws Exception;
// 正式提交阶段的逻辑
protected abstract void commit(TXN transaction);
// 取消事务，Rollback 相关的逻辑
protected abstract void abort(TXN transaction);

    
~~~



##### 4. 2PC执行流程

第一阶段：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-15-TwoPhaseCommitSinkFunction%20%E7%AC%AC%E4%B8%80%E9%98%B6%E6%AE%B5.png)

~~~
1.在状态初始化的 initializeState 方法内或者每次 Checkpoint 的 snapshotState 方法内都会调用 beginTransaction 方法开启新的事务。
2.开启新的事务后，Flink 开始处理数据，每来一条数据都会调用 invoke 方法，按照业务逻辑将数据添加到本次的事务中。
3. Checkpoint 执行 snapshotState 时，会调用 preCommit 方法进行预提交，预提交一般会对事务进行 flush 操作，到这里为止可以理解为 2PC 的第一阶段。
~~~



第二阶段：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-15-TwoPhaseCommitSinkFunction%20%E7%AC%AC%E4%BA%8C%E9%98%B6%E6%AE%B5.png)

~~~
第一阶段运行期间无论是机器故障还是 invoke 失败或者 preCommit 对应预提交的 flush 失败都可以理解为 2PC 的第一阶段返回了 No，即投票失败就会执行 2PC 第二阶段的 Rollback，对应到 TwoPhaseCommitSinkFunction 中就是执行 abort 方法，abort 方法内一般会对本次事务进行 abortTransaction 操作。

只有当 2PC 的第一阶段所有参与者都完全成功， Flink TwoPhaseCommitSinkFunction 对应的所有并行度在本次事务中 invoke 全部成功且 preCommit 对应预提交的 flush 也全部成功才认为 2PC 的第一阶段返回了Yes，即投票成功就会执行 2PC 第二阶段的 Commit，对应到 TwoPhaseCommitSinkFunction 中就是执行 Commit 方法，Commit 方法内一般会对本次事务进行 commitTransaction 操作，以上就是 Flink 中 TwoPhaseCommitSinkFunction 的大概执行流程。

在第一阶段结束时，数据被写入到了外部存储，但是当事务的隔离级别为读已提交（Read Committed）时，在外部存储中并读取不到我们写入的数据，因为并没有执行 Commit 操作。如上图图所示，是第二阶段的两种情况。
~~~



##### 5. FlinkKafkaProducer011 具体实现

~~~
1. KafkaSink 端进行预提交（preCommit）
2.1. 提交成功，向JobManager返回Yes
2.2. 提交失败，向JobManager返回No
3.JobManafer 发送Checkpoint 完成通知，最后再KafkaSink 执行notifyCheckpointComplete()，该方法内执行commit()

简单描述一下 FlinkKafkaProducer011 的实现原理：
1.FlinkKafkaProducer011 继承了 TwoPhaseCommitSinkFunction，所有并行度在 initializeState 初始化状态时，会开启新的事务，并把状态里保存的之前未提交事务进行 commit。
2.接下来开始调用 invoke 方法处理数据，会把数据通过事务 api 发送到 Kafka。一段时间后，开始 Checkpoint，checkpoint 时 snapshotState 方法会被执行，snapshotState 方法会调用 preCommit 方法并把当前还未 Commit 的事务添加到状态中来提供故障容错。
3.snapshotState 方法执行完成后，会对自身状态信息进行快照并上传到 HDFS 上来提供恢复。所有的实例都将状态信息备份完成后就认为本次 Checkpoint 结束了，此时 JobManager 会向所有的实例发送 Checkpoint 完成的通知，各实例收到通知后，会调用 notifyCheckpointComplete 方法把未提交的事务进行 commit。
4.期间如果出现其中某个并行度出现故障，JobManager 会停止此任务，向所有的实例发送通知，各实例收到通知后，调用 close 方法，关闭 Kafka 事务 Producer。
~~~



FlinkKafkaProducer011 继承了 TwoPhaseCommitSinkFunction，

如下图所示，Flink 应用使用 FlinkKafkaProducer011 时，Checkpoint 的时候不仅要将快照保存到状态后端，还要执行 preCommit 操作将缓存中的数据 flush 到 Sink 端的 Kafka 中。

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151549.jpg)

当所有的实例快照完成且所有 Sink 实例执行完 preCommit 操作时，会把快照完成的消息发送给 JobManager，JobManager 收到所有实例的 Checkpoint 完成消息时，就认为这次 Checkpoint 完成了，会向所有的实例发送 Checkpoint 完成的通知（Notify Checkpoint Completed），当 FlinkKafkaProducer011 接收到 Checkpoint 完成的消息时，就会执行 Commit 方法。

![img](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151550.jpg)

上文提到过 2PC 有一些缺点存在，关于协调者和参与者故障的问题，对应到 Flink 中如果节点发生故障会申请资源并从最近一次成功的 Checkpoint 处恢复任务，所以，节点故障的问题 Flink 已经解决了。关于 2PC 同步阻塞的问题，2PC 算法在没有等到第一阶段所有参与者的投票之前肯定是不能执行第二阶段的 Commit，所以基于 2PC 实现原理同步阻塞的问题没有办法解决，除非使用其他算法。

那数据不一致的问题呢？

在整个的第一阶段不会真正地提交数据到 Kafka，所以只要设置事务隔离识别为读已提交（Read Committed），那么第一阶段就不会导致数据不一致的问题。

那 Flink 的第二阶段呢？

Flink 中，Checkpoint 成功后，会由 JobManager 给所有的实例发送 Checkpoint 完成的通知，然后 KafkaSink 在 notifyCheckpointComplete 方法内执行 commit。假如现在执行第 n 次 Checkpoint，快照完成且预提交完成，我们认为第 n 次 Checkpoint 已经成功了，这里一定要记住无论第二阶段是否 commit 成功，Flink 都会认为第 n 次 Checkpoint 已经结束了，换言之 Flink 可能会出现第 n 次 Checkpoint 成功了，但是第 n 次 Checkpoint 对应的事务 commit 并没有成功。

当 Checkpoint 成功后，JobManager 会向所有的 KafkaSink 发送 Checkpoint 完成的通知，所有的 KafkaSink 接收到通知后才会执行 Commit 操作。假如 JobManager 发送通知时出现了故障，导致 KafkaSink 的所有并行度都没有收到通知或者只有其中一部分 KafkaSink 接收到了通知，最后有一部分的 KafkaSink 执行了 Commit，另外一部分 KafkaSink 并没有执行 Commit，此时出现了 Checkpoint 成功，但是数据并没有完整地提交到 Kafka 的情况，出现了数据不一致的问题。

那 Flink 如何解决这个问题呢？

在任务执行过程中，如果因为各种原因导致有任意一个 KafkaSink 没有 Commit 成功，就会认为 Flink 任务出现故障，就会从最近一次成功的 Checkpoint 处恢复任务，也就是从第 n 次 Checkpoint 处恢复，TwoPhaseCommitSinkFunction 将每次 Checkpoint 时需要 Commit 的事务保存在状态里，当从第 n 次 Checkpoint 恢复时会从状态中拿到第 n 次 Checkpoint 可能没有提交的事务并执行 Commit，通过这种方式来保证所有的 KafkaSink 都能将事务进行 Commit，从而解决了 2PC 协议中可能出现的数据不一致的问题。

也就是说 Flink 任务重启后，会检查之前 Checkpoint 是否有未提交的事务，如果有则执行 Commit，从而保证了 Checkpoint 之前的数据被完整地提交。





以上就是 FlinkKafkaProducer011 实现原理的简单描述，具体实现细节请参考源码。

TwoPhaseCommitSinkFunction 还存在一个问题，假如我们设置的一分钟一次 Checkpoint，事务隔离级别设置为读已提交时，那么我们这一分钟内写入的数据，都必须等到 Checkpoint 结束后，下游才能读取到，导致我们的 Flink 任务数据延迟了一分钟。所以我们要结合这个特性，合理的设置我们的 Checkpoint 周期。