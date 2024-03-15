---
date: 2024-03-16
readtime: 
categories:
  - Search
  - Performance

---


# ConfigManager

> 配置管理对象，是rocketMQ中的比较重要的一个类，就是大部分所有的配置对象都继承了这个配置管理类，我觉得有必要来介绍一下这个类，
>
> 毕竟好几个比较重要的配置类都是他的派生类

## configFilePath

配置文件的实际路径，抽象函数，由子类自己制定，父类只根据给定的路径去加载这些文件

```java
public abstract String configFilePath();
```



## decode

```java
/**
 * 将给定的json 数据解码，一般情况下是使用 fastJson 来反序列化进行解码操作的 、
 * @param jsonString
 */
public abstract void decode(final String jsonString);
```

解码函数，抽象函数，子类自己去实现如何解码，一般的json反序列化，并且，反序列化结果，需要子类自己在函数内接收处理，父类不做处理，只调用。

## encode

```java
public abstract String encode(final boolean prettyFormat);
// 抽象函数，返回一个字符串
```

就是将继承自他的子类的配置对象进行编码返回一个字符串，一般情况是 json字符串，他会将这个字符串持久化在本地磁盘上。等待解码的时候，他会将这个字符串加载出来，交给对应的子类再去做decode操作解码，序列化成一个实际的子类配置对象。

## load

> 就是加载本地磁盘的配置文件，调用decode函数，将其解码成配置对象，这是rmq的一种持久化手段

```java
public boolean load() {
    String fileName = null;
    try {
        // 获取默认配置文件路径
        fileName = this.configFilePath();
        // 根据文件全路径，读取文件内容，并将其内容以字符串形式返回
        String jsonString = MixAll.file2String(fileName);

        if (null == jsonString || jsonString.length() == 0) {
            // 如果内容是空的，则加载bak临时文件
            return this.loadBak();
        } else {
            // 调用解码函数，没有返回值，子类自己包装反序列化之后的配置对象，父类不处理
            this.decode(jsonString);
            log.info("load " + fileName + " OK");
            return true;
        }
    } catch (Exception e) {
        log.error("load " + fileName + " failed, and try to load backup file", e);
        return this.loadBak();
    }
}
```

