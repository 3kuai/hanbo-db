package com.lmx.jredis.transport.jsocket;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.lmax.disruptor.EventHandler;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.RedisServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import redis.RedisCommandDecoder;
import redis.RedisReplyEncoder;
import redis.netty4.Command;
import redis.netty4.ErrorReply;
import redis.netty4.InlineReply;
import redis.netty4.Reply;
import redis.util.BytesKey;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static redis.netty4.ErrorReply.NYI_REPLY;
import static redis.netty4.StatusReply.QUIT;

/**
 * Created by Administrator on 2017/12/16.
 */
@Slf4j
@Component
@Order(1)
public class NetEventHandler implements EventHandler<RequestEvent> {
    RedisCommandDecoder redisCommandDecoder = new RedisCommandDecoder();
    RedisReplyEncoder redisCommandEncoder = new RedisReplyEncoder();

    /**
     * 注意这里的异常要保证在处理链中try掉，否则工作线程会退出
     *
     * @param event
     * @param sequence
     * @param endOfBatch
     * @throws Exception
     */
    @Override
    public void onEvent(RequestEvent event, long sequence, boolean endOfBatch) throws Exception {
//                    log.error("event = [" + event + "], sequence = [" + sequence + "], endOfBatch = [" + endOfBatch + "]");
        if (event.getValue() instanceof SelectionKey) {
            SelectionKey key = (SelectionKey) event.getValue();
            SocketChannel socketChannel = null;
            try {
                socketChannel = (SocketChannel) key.channel();
                NioServer.BufferUtil bufUtil;
                if ((bufUtil = (NioServer.BufferUtil) key.attachment()) != null)
                    handleReq(key, bufUtil);
                key.attach(bufUtil);
                socketChannel.configureBlocking(false);
                socketChannel.register(event.getSelector(), SelectionKey.OP_WRITE);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    key.cancel();
                    socketChannel.socket().close();
                    socketChannel.close();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        } else if (event.getValue() instanceof Socket) {
            Socket socket = (Socket) event.getValue();
            try {
                byte[] req = new byte[1024];
                int length;
                while ((length = socket.getInputStream().read(req)) != -1) {
                    ByteBuf byteBuf = Unpooled.buffer(1024);
                    byteBuf.writeBytes(req, 0, length);
                    handleReq(byteBuf, socket);
                }
            } catch (Exception e) {
            }
        }
    }

    void handleReq(SelectionKey key, NioServer.BufferUtil buffer) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buf = buffer.getReadBuf();
        int len;
        if ((len = socketChannel.read(buf)) > 0) {
            buf.flip();
            byte[] bytes = new byte[1024];
            buf.get(bytes, 0, len);
            byte[] bytes_ = new byte[len];
            System.arraycopy(bytes, 0, bytes_, 0, len);
//            System.out.println("redis req= \n" + new String(bytes_));
            List request = Lists.newArrayList();
            redisCommandDecoder.decode(null, Unpooled.copiedBuffer(bytes_), request);
            for (Object o : request) {
                reply(socketChannel, o, buffer.getWriteBuf(), key);
            }
        } else {
            key.cancel();
        }
        buffer.readBuf.clear();
    }

    void handleReq(ByteBuf byteBuf, Socket socket) throws Exception {
        List request = Lists.newArrayList();
//        System.out.println("redis req= \n" + new String(byteBuf.array()));
        redisCommandDecoder.decode(null, byteBuf, request);
        for (Object o : request) {
            reply(socket, o);
        }
    }


    void reply(SocketChannel ctx, Object msg, ByteBuf byteBuf, SelectionKey key) {
        Command msg_ = (Command) msg;
        byte[] name = msg_.getName();

        for (int i = 0; i < name.length; i++) {
            byte b = name[i];
            if (b >= 'A' && b <= 'Z') {
                name[i] = (byte) (b + LOWER_DIFF);
            }
        }
        Wrapper wrapper = methods.get(new BytesKey(name));
        Reply reply = null;
        if (wrapper == null) {
            reply = new ErrorReply("unknown command '" + new String(name, Charsets.US_ASCII) + "'");
        } else {
            try {
                reply = wrapper.execute(msg_, key);
                if (reply == QUIT) {
                    try {
                        ctx.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (msg_.isInline()) {
                        if (reply == null) {
                            reply = new InlineReply(null);
                        } else {
                            reply = new InlineReply(reply.data());
                        }
                    }
                    if (reply == null) {
                        reply = NYI_REPLY;
                    }

                }
            } catch (RedisException e) {
                e.printStackTrace();
            }
        }
        try {

            redisCommandEncoder.encode(null, reply, byteBuf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void reply(Socket ctx, Object msg) {
        Command msg_ = (Command) msg;
        byte[] name = msg_.getName();

        for (int i = 0; i < name.length; i++) {
            byte b = name[i];
            if (b >= 'A' && b <= 'Z') {
                name[i] = (byte) (b + LOWER_DIFF);
            }
        }
        Wrapper wrapper = methods.get(new BytesKey(name));
        Reply reply = null;
        if (wrapper == null) {
            reply = new ErrorReply("unknown command '" + new String(name, Charsets.US_ASCII) + "'");
        } else {
            try {
                reply = wrapper.execute(msg_, null);
                if (reply == QUIT) {
                    try {
                        ctx.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (msg_.isInline()) {
                        if (reply == null) {
                            reply = new InlineReply(null);
                        } else {
                            reply = new InlineReply(reply.data());
                        }
                    }
                    if (reply == null) {
                        reply = NYI_REPLY;
                    }

                }
            } catch (RedisException e) {
                e.printStackTrace();
            }
        }
        try {
            ByteBuf byteBuf = Unpooled.buffer(1024 * 500);
            reply.write(byteBuf);
            OutputStream os = ctx.getOutputStream();
            os.write(ByteBuffer.allocate(byteBuf.readableBytes()).put(byteBuf.array(), 0, byteBuf.readableBytes()).array());
            os.flush();
            byteBuf.release();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final byte LOWER_DIFF = 'a' - 'A';
    private Map<BytesKey, Wrapper> methods = new HashMap();

    interface Wrapper {

        Reply execute(Command command, SelectionKey key) throws RedisException;
    }

    public void initRedis(final RedisServer rs) {
        final Class<? extends RedisServer> aClass = rs.getClass();
        for (final Method method : aClass.getMethods()) {
            final Class<?>[] types = method.getParameterTypes();
            methods.put(new BytesKey(method.getName().getBytes()), new Wrapper() {
                @Override
                public Reply execute(Command command, SelectionKey key) throws RedisException {
                    Object[] objects = new Object[types.length];
                    long start = System.currentTimeMillis();
                    try {
                        command.toArguments(objects, types);
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
            });
        }
    }
}
