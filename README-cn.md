# HanboDB
java写的redis服务端程序

## 特征
支持数据动态扩容  
支持所有redis原生协议

    1.set get
    2.lpush rpush lrange blpop brpop llen
    3.hset hget hgetall hscan
    4.pub sub
    5.select keys scan expire del
    6.multi exec discard
    7.incr incrby
    8.slaveof
    ....
    
## 客户端
    兼容jedis，spring-data-redis
    redis桌面版0.9+
    
## 架构
仅key占用JVM堆内存空间，value使用堆外内存（内存文件映射）

![存储设计](https://github.com/lmx1989219/jredis/blob/master/storage-design.png)

![buffer结构](https://github.com/lmx1989219/jredis/blob/master/buffer-structure.png)

![主从复制](https://github.com/lmx1989219/jredis/blob/master/replication.png)

### 线性存储
    1.头四个字节（存储offset）存放buffer最后的写入位置，后续的单元（数据内容）格式为TLV格式（flag+length+valueBytes）
    2.k/v数据独立存储
    3.超过阀值会自动扩容为原始空间的2倍大小
### 构建
    cd jredis && mvn clean install -Dmaven.test.skip=true
### 运行
    java -jar jredis-{version}.jar
    
### 高可用配置
支持一主多从，不过仅支持手动切换主从关系，计划未来实现哨兵机制    
#### 主节点
自动发现从节点
    
    replication.mode=master
    
#### 从节点
主动注册从节点
    
    replication.mode=slave
    slaver.of=127.0.0.1:16379