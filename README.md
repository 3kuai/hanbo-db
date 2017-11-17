# jredis
redis of java implement

## features
    1.set get
    2.lpush rpush lrange
    3.hset hget hgetall
    4.publish subscriber
    5.keys expire del
    
## architecture
### storage
    1.based on MappedByteBuffer
    2.fixed length unit,head 4 bytes in last item position,and then each item in 4 bytes with it's bytes length and actual bytes.
    3.key and value are seperate in storage
### protocol
    1.based on redis protocol
    2.based on netty codec
### transport
    based on netty 4
### io    
    single accepter and single worker
