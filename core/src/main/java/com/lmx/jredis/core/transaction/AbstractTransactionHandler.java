package com.lmx.jredis.core.transaction;

import com.lmx.jredis.core.BusHelper;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.RedisCommandInvoker;
import com.lmx.jredis.core.RedisCommandProcessor;
import com.lmx.jredis.core.datastruct.RedisDbDelegate;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import redis.netty4.Command;
import redis.netty4.IntegerReply;
import redis.netty4.Reply;
import redis.netty4.StatusReply;

import java.util.ArrayDeque;
import java.util.Queue;

import static redis.netty4.IntegerReply.integer;

/**
 * 加入会话层
 * <p>
 * 开启db分区（默认第一个分区）和事务管理
 * <p>
 * Created by limingxin on 2017/12/20.
 */
@Slf4j
public abstract class AbstractTransactionHandler implements RedisCommandProcessor {
    protected BusHelper bus;
    protected RedisDbDelegate delegate;
    protected ChannelHandlerContext channelHandlerContext;
    protected String transaction = "transactionIdentify";
    protected String session = "sessionIdentify";

    protected Attribute getTxAttribute() {
        return channelHandlerContext.channel().attr(AttributeKey.valueOf(transaction));
    }

    public boolean hasOpenTx() {
        return getTxAttribute().get() != null;
    }


    public Reply handlerTxOp(Command command) throws RedisException {
        if (new String(command.getName()).equals("exec")) {
            return exec();
        }
        if (new String(command.getName()).equals("discard")) {
            return discard();
        }
        Queue queue = (Queue) getTxAttribute().get();
        //declare internal event for command
        command.setEventType(1);
        queue.add(command);
        return StatusReply.QUEUED;
    }


    @Override
    public StatusReply multi() throws RedisException {
        Attribute attribute = getTxAttribute();
        attribute.set(new ArrayDeque());
        return StatusReply.OK;
    }

    @Override
    public StatusReply exec() throws RedisException {
        Attribute attribute = getTxAttribute();
        Queue queue = ((Queue) attribute.get());
        for (Object o : queue) {
            RedisCommandInvoker.handlerEvent(channelHandlerContext, (Command) o);
        }
        queue.clear();
        attribute.set(null);
        return StatusReply.OK;
    }

    @Override
    public StatusReply discard() throws RedisException {
        Attribute attribute = getTxAttribute();
        ((Queue) attribute.get()).clear();
        attribute.set(null);
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
            attribute.set(null);
            throw new RedisException();
        }
        attribute.set(store);
        return StatusReply.OK;
    }

    public IntegerReply subscribe(byte[][] channel) {
        bus.regSubscriber(channelHandlerContext, channel);
        return integer(1);
    }

    public void initStore(BusHelper bus, RedisDbDelegate delegate) {
        this.bus = bus;
        this.delegate = delegate;
    }

}
