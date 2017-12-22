package com.lmx.jredis.transport;

import com.google.common.base.Charsets;
import com.lmx.jredis.core.BusHelper;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.RedisServer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.netty4.*;
import redis.util.BytesKey;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import static redis.netty4.ErrorReply.NYI_REPLY;
import static redis.netty4.StatusReply.QUIT;

@Slf4j
@ChannelHandler.Sharable
@Component
public class NettyServerHandler extends SimpleChannelInboundHandler<Command> {

    @Autowired
    private BusHelper busHelper;
    private Map<BytesKey, Wrapper> methods = new HashMap();

    public interface Wrapper {
        Reply execute(Command command, ChannelHandlerContext ch) throws RedisException;

        Reply execute(Command command, SocketChannel ch) throws RedisException;
    }

    public void init(final RedisServer rs) {
        final Class<? extends RedisServer> aClass = rs.getClass();
        for (final Method method : aClass.getMethods()) {
            final Class<?>[] types = method.getParameterTypes();
            methods.put(new BytesKey(method.getName().getBytes()), new Wrapper() {
                @Override
                public Reply execute(Command command, ChannelHandlerContext ch) throws RedisException {
                    Object[] objects = new Object[types.length];
                    long start = System.currentTimeMillis();
                    try {
                        command.toArguments(objects, types);
                        rs.setChannelHandlerContext(ch);
                        return (Reply) method.invoke(rs, objects);
                    } catch (IllegalAccessException e) {
                        throw new RedisException("Invalid server implementation");
                    } catch (InvocationTargetException e) {
                        Throwable te = e.getTargetException();
                        if (!(te instanceof RedisException)) {
                            te.printStackTrace();
                        }
                        return new ErrorReply("ERR " + te.getMessage());
                    } catch (Exception e) {
                        return new ErrorReply("ERR " + e.getMessage());
                    } finally {
                        log.info("method {},cost {}ms", method.getName(), (System.currentTimeMillis() - start));
                    }
                }

                @Override
                public Reply execute(Command command, SocketChannel ch) throws RedisException {
                    return StatusReply.OK;
                }
            });
        }
    }

    private static final byte LOWER_DIFF = 'a' - 'A';

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
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
        if (reply == QUIT) {
            ctx.close();
        } else {
            if (msg.isInline()) {
                if (reply == null) {
                    reply = new InlineReply(null);
                } else {
                    reply = new InlineReply(reply.data());
                }
            }
            if (reply == null) {
                reply = NYI_REPLY;
            }
            ctx.write(reply);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        log.error("", cause);
        busHelper.unSubscriber(ctx);
        ctx.close();
    }
}
