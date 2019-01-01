package com.lmx.jredis.core.transaction;

import com.lmx.jredis.core.BusHelper;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.RedisServer;
import com.lmx.jredis.core.datastruct.RedisDbDelegate;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import redis.netty4.IntegerReply;
import redis.netty4.Reply;
import redis.netty4.StatusReply;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import static redis.netty4.IntegerReply.integer;

/**
 * 加入会话层
 * <p>
 * 开启db分区（默认第一个分区）和事务管理
 * <p>
 * Created by limingxin on 2017/12/20.
 */
@Slf4j
public abstract class AbstractTransactionHandler implements RedisServer {
    protected BusHelper bus;
    protected RedisDbDelegate delegate;
    protected ChannelHandlerContext channelHandlerContext;
    protected String transaction = "transactionIdentify";
    protected String session = "sessionIdentify";
    protected Map<String, Method> methods = new ConcurrentHashMap<>();
    protected TxOperation txOperation = (TxOperation) new TxOperationProxy().newProxy(TxOperation.class);

    {
        for (int i = 0; i < TxOperation.class.getMethods().length; i++) {
            Method method = TxOperation.class.getMethods()[i];
            methods.put(method.getName(), method);
        }

    }

    protected Attribute getTxAttribute() {
        return channelHandlerContext.channel().attr(AttributeKey.valueOf(transaction));
    }

    class TxOperationProxy implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Queue queue = (Queue) getTxAttribute().get();
            if (method.getName().equals("set")) {
                queue.add(QueueEvent.builder().type("set").key((byte[]) args[0]).value((byte[]) args[1]).build());
                return StatusReply.QUEUED;
            }
            //TODO more operations
            if (method.getName().equals("exec"))
                return exec();
            if (method.getName().equals("discard"))
                return discard();
            return null;
        }

        public Object newProxy(Class clz) {
            return Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{clz}, this);
        }
    }

    public boolean hasOpenTx() {
        return getTxAttribute().get() != null;
    }

    public Reply handlerTxOp(byte[] cmd, Object[] objects) {
        try {
            return (Reply) methods.get(new String(cmd)).invoke(txOperation, objects);
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
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
            QueueEvent queueEvent = (QueueEvent) o;
            RedisDbDelegate.RedisDB kv = getRedisDB();
            if (queueEvent.getType().equals("set"))
                kv.getSimpleKV().write(new String(queueEvent.getKey()), new String(queueEvent.getValue()));
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
