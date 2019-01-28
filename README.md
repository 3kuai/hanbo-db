# jredis
redis of java implemented,same as a redis server.

## features
supported command list

    1.set get
    2.lpush rpush lrange blpop brpop llen
    3.hset hget hgetall hscan
    4.pub sub
    5.select keys scan expire del
    6.multi exec discard
    7.incr incrby
    ....
    
## CLI TOOL
    support jedis,spring-data-redis,spring-boot-starter-redis
    support redisDesktop management tool with v0.9+
    
## architecture
only keys be used in RAM,the value is lazy load

![storage design](https://github.com/lmx1989219/jredis/blob/master/storage-design.png)

![buffer-structure](https://github.com/lmx1989219/jredis/blob/master/buffer-structure.png)

### evict policy
    LRU algorithm
### linear storage
    1.based on jdk's MappedByteBuffer
    2.fixed unit size ,head 4 byte write in last item position,
    and then each item write in 4 byte with it bytes length and actually bytes.
    3.key and value are seperated in storage
### protocol
    redis protocol
### transport
    based on netty 4
### thread model
    single worker
### build server
    cd jredis && mvn clean install
### run server
 jredis build in a springboot application,so easy to run like follow
 
    java -jar jredis-{version}.jar
    
