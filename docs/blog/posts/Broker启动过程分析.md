---
date: 2024-01-31
 
readtime: 15
categories:
  - Search
  - Performance
---

---



# 1. 启动入口

org.apache.rocketmq.broker.BrokerStartup#main

执行main函数之后，创建控制器

org.apache.rocketmq.broker.BrokerStartup#createBrokerController



## 1.1 brokerController介绍

broker控制器，就是broker核心管理组件，

包含了几个核心配置：

- BrokerConfig: broker基础配置

- NettyServerConfig：nettyserver的配置

- NettyClientConfig

- MessageStoreConfig： [消息存储配置](./brokerController组件介绍/MessageStoreConfig.md)

- RebalanceLockManager： [消费者客户端重平衡时候的绑定关系管理](./brokerController组件介绍/RebalanceLockManager.md)

- ConsumerOffsetManager：   [消费偏移管理器](./brokerController组件介绍/ConsumerOffsetManager.md)

- TopicConfigManager：  [topic管理器 ](./brokerController组件介绍/TopicConfigManager.md)

- PullMessageProcessor：消息拉取处理器

- PullRequestHoldService

- NotifyMessageArrivingListener：消息到达通知器

- DefaultConsumerIdsChangeListener：没看懂干啥的、

- ConsumerManager：消费者管理器===具体作用不知道，

- ConsumerFilterManager：这个肯定是管理消费者的过滤器的

- ProducerManager：生产者管理 ,详细介绍： [ProducerManager介绍](./brokerController组件介绍/ProducerManager介绍.md) 

  

  

- ClientHousekeepingService：管理客户端是否掉线吧。

- Broker2Client

- SubscriptionGroupManager：订阅组

- BrokerOuterAPI：broker外部api

- FilterServerManager：

- SlaveSynchronize：salve同步

- BrokerStatsManager：broker状态管理

- BrokerFastFailure：

- Configuration：

## 1.2 创建和初始化brokerController

### 1. 加载本地配置文件

```java
// topic配置
boolean result = this.topicConfigManager.load();
// 消费偏移
result = result && this.consumerOffsetManager.load();
// 订阅关系
result = result && this.subscriptionGroupManager.load();
// 消费者的filter
result = result && this.consumerFilterManager.load();
```

### 2.DefaultMessageStore消息存储对象初始化



```java
/// 创建默认的存储对象
// 使用几个 commitLog， index reput 之类的 类协调底层的存储逻辑、
this.messageStore =
    new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener, this.brokerConfig);
```

***DefaultMessageStore***：

> 默认的消息存储管理组件
>
> rocketMq的存储核心组件，可以说是最重要的组件，没有之一，rmq 主要功能就是网络，存储。

 [消息存储管理组件DefaultMessageStore](./brokerController组件介绍/DefaultMessageStore.md)

### 3. DLedgerRoleChangeHandler 支持

> 这个新特性我没有接触过，可以了解一下。

```java
if (messageStoreConfig.isEnableDLegerCommitLog()) {
    DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(this, (DefaultMessageStore) messageStore);
    ((DLedgerCommitLog)((DefaultMessageStore) messageStore).getCommitLog()).getdLedgerServer().getdLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
}
```





### 4. 拦截器的添加

> rmq接收到消息之后，要进行一系列的构建索引，分发到不同的messageQueue之类的操作，就是通过 reput 线程做的，他好像就是一个拦截器，rmq收到消息后会执行 这里添加的拦截器，可以关注下，有助于了解rmq的消息转发流程，但是不是太难也不是高级的技术。因此知道即可。

```java
// 添加消息分发的过滤器。 这个默认的 是 支持 标签过滤的， 即rocket消费者 可以在订阅的时候传递表达式，
// 这里就是计算消息和那个表达式的match关系的，match的发给消费者，不match的不发给消费者
// todo: 每条消息都要经过所有的这些分发器，可以用它来分发消息，同样，也可以用它来过滤消息，或者构建索引
this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
```



其实为了佐证我上面的猜想，可以去messageStore的类中去看是不是添加了默认的消息拦截器

org.apache.rocketmq.store.DefaultMessageStore#DefaultMessageStore

```java
this.dispatcherList = new LinkedList<>();
this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());
```

可以看到，添加了两个默认的消息拦截器，构建consumerQueue和索引，messageQueue不是，可以后续看下messageQueue中的消息是如何构建的，我现在忘了，记不清了。



### 5. messageStore 加载commitLog 文件

这一步会加载commitLog，index 之类的本地文件，将磁盘文件和本地内存建立内存映射关系

org.apache.rocketmq.store.DefaultMessageStore#load

即，作用是保证本地磁盘文件的完整性。



### 6. 网络相关组件的初始化

1. .**remotingServer** ：提供web服务的服务器
2. .fastRemotingServer：和上面一样，但是不知道是什么意思

### 7. 启动相关的线程池

建立了很多的线程池，不知道有啥用？这就是线程隔离？在大型项目或者高端配置的服务器有作用，在配置较低的机器上这也没啥用。

不过话说回来，mq肯定不会放在配置低的机器上。



### 8. 注册消息处理器

```java
// 注册消息的处理器， 用于处理netty收到的请求
this.registerProcessor();
```

rmq 收到消息后，根据rmq的消息协议，一定有一个消息code，他就是根据消息code来获取对应的消息处理器，

从而将请求转发给对应的消息处理器，进行消息的处理。

### 9. 启动定时任务



1. 持久化的
2. 状态记录的
3. 更新name server 地址的



### 10. 初始化事务管理器  initialTransaction

rmq的事务消息叫half消息，即发送到rmq后必须在确认一次才算提交成功。



## 1.3 初始化失败即将brokerController ShuntDown

```java
controller.shutdown();
```

如果broklerController 初始化失败了，那么需要将控制器启动的响应的资源释放，即调用函数的shutdown() 函数。





# 2. 启动 BrokerController

org.apache.rocketmq.broker.BrokerController#start

## 2.1 启动messageStore组件

1. 设置锁文件
2. 更新消费者读取的最大的位置，如果消费者消费的最大位置小于commitLog的最小位置，就以最小的位置开始消费，**之前没消费的消息就丢失**
3. 启动reput组件，进行消息转发
4. **启动ha**
5. .**FlushConsumeQueueService** 任务启动
6. .**commitLog 服务启动**，可以细看
7. .StoreStatsService ： 状态监控打印，不太重要，但是作为监控很有用
8. 启动定时任务：
   1. 删除过期的commitLog
   2. 删除consumerQueue 文件
   3. 磁盘空间检查

## 2.2 启动网络服务组件

org.apache.rocketmq.broker.BrokerController#start

### 2.2.1 remotingServer

启动netty服务，org.apache.rocketmq.remoting.netty.NettyRemotingServer#start

### 2.2.2 文件监控服务 FileWatchService

org.apache.rocketmq.common.ServiceThread#start

如果被监控的文件的 md5变了，证明文件被修改了。。。

### 2.2.3 RemotingClient 客户端启动，服务端是否需要客户端？

### 2.2.4 ：pullRequestHoldService

剩下的感觉不太重要咯。。。。



















# END：各组件介绍



