[]()[中文](https://github.com/3kuai/jredis/edit/master/README-cn.md)
[]()[english](https://github.com/3kuai/jredis/edit/master/README.md)
# HanboDB
hanboDB is a high available,low latency memory database system.

## features
1. support dynamic resize 
2. support all of redis proto,e.g:

        1.set get
        2.lpush rpush lrange blpop brpop llen
        3.hset hget hgetall hscan
        4.pub sub
        5.select keys scan expire del
        6.multi exec discard
        7.incr incrby
        8.slaveof
        ....  
    
## architecture
use fastUtil library(Object2List,Object2Map) as keyMap in JVM,it has a powerful read/write ability.

![storage design](https://github.com/3kuai/hanbo-db/blob/master/storage-design.png)

![buffer-structure](https://github.com/3kuai/hanbo-db/blob/master/buffer-structure.png)

![replication](https://github.com/3kuai/hanbo-db/blob/master/replication.png)

### linear storage
    1.fixed unit size ,head 4 byte write in last item position,
    and then each item write in 4 byte with it bytes length and actually bytes(TLV).
    2.key and value are seperated in storage
    3.if buffer remaining not enough to write,then auto resize
### build server
    cd jredis && mvn clean install -Dmaven.test.skip=true
### run server
    java -jar transport-1.0-SNAPSHOT.jar
    
### ha conf 
support one master multi slave,but not support auto selective master,now need to manual promotion.
i will plan to implement sentinel mechanism 
#### master
auto discovery slave node

    replication.mode=master
    
#### slave
auto register slave node

    replication.mode=slave
    slaver.of=127.0.0.1:16379
