package com.lmx.jredis.core;

import com.google.common.base.Charsets;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import redis.netty4.Command;
import redis.netty4.ErrorReply;
import redis.netty4.Reply;
import redis.util.BytesKey;

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

    public static final Map<BytesKey, Wrapper> methods = new ConcurrentHashMap<>();

    public interface Wrapper {
        Reply execute(Command command, ChannelHandlerContext ch) throws RedisException;
    }

    public void init(final RedisCommandProcessor rs) {
        final Class<? extends RedisCommandProcessor> aClass = rs.getClass();
        for (final Method method : aClass.getMethods()) {
            final Class<?>[] types = method.getParameterTypes();
            methods.put(new BytesKey(method.getName().getBytes()), new Wrapper() {
                @Override
                public Reply execute(Command command, ChannelHandlerContext ch) {
                    Object[] objects = new Object[types.length];
                    long start = System.currentTimeMillis();
                    try {
                        command.toArguments(objects, types);
                        rs.setChannelHandlerContext(ch);
                        if (rs.hasOpenTx() && command.getEventType() == 0) {
                            return rs.handlerTxOp(command);
                        } else {
                            return (Reply) method.invoke(rs, objects);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        return new ErrorReply("ERR " + e.getMessage());
                    } finally {
                        log.info("method {},cost {}ms", method.getName(), (System.currentTimeMillis() - start));
                    }
                }

            });
        }
    }

    public static Reply handlerEvent(ChannelHandlerContext ctx, Command msg) throws RedisException {
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
        } else {
            reply = wrapper.execute(msg, ctx);
        }
        return reply;
    }
}
