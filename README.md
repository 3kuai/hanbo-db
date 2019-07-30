[]()[中文](https://github.com/3kuai/jredis/edit/master/README-cn.md)
[]()[english](https://github.com/3kuai/jredis/edit/master/README.md)
# HanboDB
redis of java implemented, as same as a redis server.

## features
support a part of redis proto,e.g:

    1.set get
    2.lpush rpush lrange blpop brpop llen
    3.hset hget hgetall hscan
    4.pub sub
    5.select keys scan expire del
    6.multi exec discard
    7.incr incrby
    8.slaveof
    ....
support dynamic resize disk space    

## client side
    compat jedis,spring-data-redis,spring-boot-starter-redis
    compat redisDesktop management tool with v0.9+
    
## architecture
only keys be used in JVM

![storage design](https://github.com/lmx1989219/jredis/blob/master/storage-design.png)

![buffer-structure](https://github.com/lmx1989219/jredis/blob/master/buffer-structure.png)

![replication](https://github.com/lmx1989219/jredis/blob/master/replication.png)

### linear storage
    1.based on jdk's MappedByteBuffer
    2.fixed unit size ,head 4 byte write in last item position,
    and then each item write in 4 byte with it bytes length and actually bytes(TLV).
    3.key and value are seperated in storage
### build server
    cd jredis && mvn clean install
### run server
 jredis build in a springboot application,so easy to run like follow
 
    java -jar jredis-{version}.jar
    
### master conf
auto discovery slave node

    replication.mode=master
    
### slave conf
auto register slave node

    slaver.of=127.0.0.1:16379