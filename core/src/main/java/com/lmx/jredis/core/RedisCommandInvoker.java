package com.lmx.jredis.core;

import com.google.common.base.Charsets;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import redis.netty4.Command;
import redis.netty4.ErrorReply;
import redis.netty4.Reply;
import redis.util.BytesKey;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author : lucas
 * @Description: redis 调用器
 * @date Date : 2019/01/01 16:03
 */
@Slf4j
@Component
public class RedisCommandInvoker {
    private static final byte LOWER_DIFF = 'a' - 'A';
    private RedisCommandProcessor rs;
    public static final Map<BytesKey, Wrapper> methods = new ConcurrentHashMap<>();

    public interface Wrapper {
        Reply execute(Command command, ChannelHandlerContext ch) throws RedisException;
    }

    public void init(RedisCommandProcessor rss) {
        this.rs = rss;
        final Class<? extends RedisCommandProcessor> aClass = rs.getClass();
        for (final Method method : aClass.getMethods()) {
            final Class<?>[] parameterTypes = method.getParameterTypes();
            final String mName = method.getName();
            methods.put(new BytesKey(mName.getBytes()), new Wrapper() {
                @Override
                public Reply execute(Command command, ChannelHandlerContext ch) {
                    Object[] objects = new Object[parameterTypes.length];
                    long start = System.currentTimeMillis();
                    try {
                        command.toArguments(objects, parameterTypes);
                        //check param
                        if (command.getObjects().length - 1 < parameterTypes.length) {
                            throw new RedisException("wrong number of arguments for '" + mName + "' command");
                        }
                        rs.setChannelHandlerContext(ch);
                        if (rs.hasOpenTx() && command.isInternal()) {
                            return rs.handlerTxOp(command);
                        } else {
                            return (Reply) method.invoke(rs, objects);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        if (e instanceof InvocationTargetException)
                            return new ErrorReply("ERR " + ((InvocationTargetException) e).getTargetException().getMessage());
                        else
                            return new ErrorReply("ERR " + e.getMessage());
                    } finally {
                        log.info("method {},cost {}ms", method.getName(), (System.currentTimeMillis() - start));
                    }
                }

            });
        }
    }

    public Reply handlerEvent(ChannelHandlerContext ctx, Command msg) throws RedisException {
        byte[] name = msg.getName();

        for (int i = 0; i < name.length; i++) {
            byte b = name[i];
            if (b >= 'A' && b <= 'Z') {
                name[i] = (byte) (b + LOWER_DIFF);
            }
        }
        Wrapper wrapper = methods.get(new BytesKey(name));
        Reply reply;
        if (wrapper == null) {
            reply = new ErrorReply("unknown command '" + new String(name, Charsets.US_ASCII) + "'");
            //if started tx,then invoker discard method and setErrorMsg
            if (rs.hasOpenTx()) {
                rs.discard();
                rs.setTXError();
            }
        } else {
            reply = wrapper.execute(msg, ctx);
        }
        return reply;
    }
}
