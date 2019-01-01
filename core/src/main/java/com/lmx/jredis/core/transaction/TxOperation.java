package com.lmx.jredis.core.transaction;

import redis.netty4.Reply;

/**
 * @author : lucas
 * @Description: 事务操作接口
 * @date Date : 2019/01/01 16:14
 */
public interface TxOperation {
    Reply exec();

    Reply discard();

    Reply set(byte[] k, byte[] v);

    Reply get(byte[] k);

    Reply expire(byte[] k);

    Reply lpush(byte[] k, byte[]... v);

    Reply rpush(byte[] k, byte[]... v);

    Reply hset(byte[] hash, byte[] field, byte[] v);

    Reply hget(byte[] hash, byte[] field);

    Reply hgetall(byte[] hash);
}
