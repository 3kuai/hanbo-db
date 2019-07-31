[]()[中文](https://github.com/3kuai/jredis/edit/master/README-cn.md)
[]()[english](https://github.com/3kuai/jredis/edit/master/README.md)
# HanboDB
redis of java implemented, as same as a redis server.

## features
support dynamic resize 
support all of redis proto,e.g:

    1.set get
    2.lpush rpush lrange blpop brpop llen
    3.hset hget hgetall hscan
    4.pub sub
    5.select keys scan expire del
    6.multi exec discard
    7.incr incrby
    8.slaveof
    ....  

## client side
    compat jedis,spring-data-redis
    compat redisDesktop management tool with v0.9+
    
## architecture
only keys be loaded in JVM heap

![storage design](https://github.com/lmx1989219/jredis/blob/master/storage-design.png)

![buffer-structure](https://github.com/lmx1989219/jredis/blob/master/buffer-structure.png)

![replication](https://github.com/lmx1989219/jredis/blob/master/replication.png)

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