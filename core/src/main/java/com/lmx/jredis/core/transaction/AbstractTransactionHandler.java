package com.lmx.jredis.core.transaction;

import com.lmx.jredis.core.BusHelper;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.RedisServer;
import com.lmx.jredis.core.datastruct.RedisDbDelegate;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import redis.netty4.IntegerReply;
import redis.netty4.StatusReply;

import java.util.ArrayDeque;
import java.util.Queue;

import static redis.netty4.IntegerReply.integer;

/**
 * 加入会话层
 * <p>
 * 开启db分区（默认第一个分区）和事物管理
 * <p>
 * Created by limingxin on 2017/12/20.
 */
public abstract class AbstractTransactionHandler implements RedisServer {
    protected BusHelper bus;
    protected RedisDbDelegate delegate;
    protected ChannelHandlerContext channelHandlerContext;
    protected String transaction = "transactionIdentify";
    protected String session = "sessionIdentify";

    protected Attribute getTxAttribute() {
        return channelHandlerContext.channel().attr(AttributeKey.valueOf(transaction));
    }

    @Override
    public StatusReply multi() throws RedisException {
        Attribute attribute = getTxAttribute();
        attribute.set(new ArrayDeque());
        return StatusReply.OK;
    }

    @Override
    public StatusReply discard() throws RedisException {
        Attribute attribute = getTxAttribute();
        ((Queue) attribute.get()).clear();
        attribute.remove();
        return StatusReply.OK;
    }

    public void setChannelHandlerContext(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    public RedisDbDelegate.RedisDB getRedisDB() {
        RedisDbDelegate.RedisDB redisDB = (RedisDbDelegate.RedisDB) channelHandlerContext.channel().attr(AttributeKey.valueOf(session)).get();
        return (redisDB == null ? delegate.select(0) : redisDB);
    }

    /**
     * Change the selected database for the current connection
     * Connection
     *
     * @param index0
     * @return StatusReply
     */
    @Override
    public StatusReply select(byte[] index0) throws RedisException {
        RedisDbDelegate.RedisDB store = delegate.select(Integer.parseInt(new String(index0)));
        Attribute attribute = channelHandlerContext.channel().attr(AttributeKey.valueOf(session));
        if (null == store) {
            attribute.remove();
            throw new RedisException();
        }
        attribute.set(store);
        return StatusReply.OK;
    }

    public IntegerReply subscribe(byte[][] channel) {
        bus.regSubscriber(channelHandlerContext, channel);
        return integer(1);
    }

    public StatusReply set(byte[] key0, byte[] value1) throws RedisException {
        Queue queue = (Queue) getTxAttribute().get();
        if (queue != null) {
            queue.add(QueueEvent.builder().type("set").key(key0).value(value1).build());
            return StatusReply.QUEUED;
        } else {
            return null;
        }
    }
}
