package com.lmx.jredis.core.transaction;

import com.google.common.base.Charsets;
import com.lmx.jredis.core.PubSubHelper;
import com.lmx.jredis.core.RedisCommandInvoker;
import com.lmx.jredis.core.RedisCommandProcessor;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.datastruct.DatabaseRouter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import redis.netty4.*;

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
    protected PubSubHelper bus;
    protected DatabaseRouter delegate;
    protected ChannelHandlerContext channelHandlerContext;
    private String transaction = "transactionIdentify";
    private String session = "sessionIdentify";
    private RedisCommandInvoker invoker;
    private AttributeKey txErrorAttr = AttributeKey.valueOf("txError");

    public Attribute getTxAttribute() {
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
    public Reply exec() throws RedisException {
        Attribute attribute = getTxAttribute();
        if (attribute.get() == null) {
            return ErrorReply.ERROR_MULTI;
        }
        Queue queue = ((Queue) attribute.get());
        if (queue != null) {
            BulkReply[] bulkReply = new BulkReply[queue.size()];
            int i = 0;
            for (Object o : queue) {
                Reply reply = invoker.handlerEvent(channelHandlerContext, (Command) o);
                if (reply instanceof IntegerReply) {
                    Long resp = ((IntegerReply) reply).data();
                    bulkReply[i++] = new BulkReply(new byte[]{resp.byteValue()});
                }
                if (reply instanceof StatusReply) {
                    String resp = ((StatusReply) reply).data();
                    bulkReply[i++] = new BulkReply(resp.getBytes(Charsets.UTF_8));
                }
            }
            queue.clear();
            attribute.set(null);
            return new MultiBulkReply(bulkReply);
        } else {
            String msg;
            if ((msg = (String) channelHandlerContext.channel().attr(txErrorAttr).get()) != null) {
                channelHandlerContext.channel().attr(txErrorAttr).set(null);
                return new ErrorReply(msg);
            }
        }
        return MultiBulkReply.EMPTY;
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

    public DatabaseRouter.RedisDB getRedisDB() {
        DatabaseRouter.RedisDB redisDB = (DatabaseRouter.RedisDB) channelHandlerContext.channel().attr(AttributeKey.valueOf(session)).get();
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
        DatabaseRouter.RedisDB store = delegate.select(Integer.parseInt(new String(index0)));
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

    public void initStore(PubSubHelper bus, DatabaseRouter delegate, RedisCommandInvoker invoker) {
        this.bus = bus;
        this.delegate = delegate;
        this.invoker = invoker;
    }

    public void setTXError() {
        channelHandlerContext.channel().attr(txErrorAttr).set("EXECABORT Transaction discarded because of previous errors.");
    }
}
