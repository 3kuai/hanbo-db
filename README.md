# jredis
redis of java implement

## features
  
    1.set get
    2.lpush rpush lrange blpop brpop llen
    3.hset hget hgetall hscan
    4.pub sub
    5.keys scan expire del
    6.multi exec discard
    ....
    
    7.support redisDesktop management tool with v0.9+
    
## architecture
### storage
    1.based on MappedByteBuffer
    2.fixed length unit,head 4 byte write in last item position,and then each item write in 4 byte with it bytes length and actual byte number.
    3.key and value are seperated in storage
### protocol
    1.compati redis protocol (support more part of redis protocol)
    2.based on netty codec
### transport
    based on netty 4
### event model
    single accepter and single worker

### build server
    cd jredis && maven clean install
### run server
 jredis build in a springboot application,so easy to run like follow
 
    java -jar jredis-{version}.jar
