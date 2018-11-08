# jredis
redis of java implement

## features
  
    1.set get.
    2.lpush rpush lrange   
    3.hset hget hgetall    
    4.pub sub    
    5.keys expire del
    6.multi exec discard
    ....
    
## architecture
### storage
    1.based on MappedByteBuffer
    2.fixed length unit,head 4 byte write in last item position,
    and then each item write in 4 byte with it bytes length and actual byte number.
    3.key and value are seperated in storage
### protocol
    1.compati redis protocol (support a part of redis protocol)
    2.based on netty codec
### transport
    based on netty 4
### event model
    single accepter and single worker
