# jredis
redis of java implemention

## architecture
### storage
    1.based on MappedByteBuffer
    2.fixed length unit,head 4 bytes in last item position,and then every item in it 4 bytes for it's bytes length and it's self bytes.
    3.key and value are seperatored in storage
### protocol
    1.based on redis protocol
    2.based on netty codec
### transport
    1.based on netty 4
    2.single accepter and single worker
