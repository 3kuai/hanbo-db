package com.lmx.jredis.transport.jsocket;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.RedisServer;
import com.lmx.jredis.core.SimpleNioRedisServer;
import com.lmx.jredis.core.datastruct.SimpleStructDelegate;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.RedisCommandDecoder;
import redis.netty4.Command;
import redis.netty4.ErrorReply;
import redis.netty4.InlineReply;
import redis.netty4.Reply;
import redis.util.BytesKey;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static redis.netty4.ErrorReply.NYI_REPLY;
import static redis.netty4.StatusReply.QUIT;

/**
 * Created by limingxin on 2017/12/14.
 */
@Service
@Slf4j
public class Server {
    @Autowired
    SimpleStructDelegate simpleStructDelegate;
    RequestEventProducer requestEventProducer = new RequestEventProducer();
    RedisCommandDecoder redisCommandDecoder = new RedisCommandDecoder();

    @PostConstruct
    public void init() {

        try {
            SimpleNioRedisServer simpleNioRedisServer = new SimpleNioRedisServer();
            simpleNioRedisServer.setDelegate(simpleStructDelegate);
            initRedis(simpleNioRedisServer);

            Disruptor<RequestEvent> disruptor = new Disruptor(RequestEvent.FACTORY, 2 << 16, DaemonThreadFactory.INSTANCE);
            disruptor.handleEventsWith(new EventHandler<RequestEvent>() {
                /**
                 * 注意这里的异常要保证在处理链中try掉，否则工作线程会退出
                 * @param event
                 * @param sequence
                 * @param endOfBatch
                 * @throws Exception
                 */
                @Override
                public void onEvent(RequestEvent event, long sequence, boolean endOfBatch) throws Exception {
//                    System.out.println("event = [" + event + "], sequence = [" + sequence + "], endOfBatch = [" + endOfBatch + "]");
                    if (event.getValue() instanceof SelectionKey) {
                        SelectionKey key = (SelectionKey) event.getValue();
                        try {
                            handleReq((SocketChannel) key.channel());
                        } catch (Exception e) {
                            key.cancel();
                            ((SocketChannel) key.channel()).socket().close();
                            key.channel().close();
                            log.error("", e);
                        }
                    }
                }
            });
            RingBuffer ringBuffer = disruptor.start();
            requestEventProducer.setRingBuffer(ringBuffer);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        startServer();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final byte LOWER_DIFF = 'a' - 'A';
    private Map<BytesKey, Wrapper> methods = new HashMap();

    void startServer() throws Exception {
        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        InetSocketAddress address = new InetSocketAddress("0.0.0.0", 16380);
        System.err.printf("jRedis nio server listening on port=%d \n", 16380);
        serverSocketChannel.bind(address);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        boolean flag = false;
        while (true) {
            int readyChannels = selector.select();
            if (flag)
                break;
            if (readyChannels == 0) {
                Thread.sleep(50L);
                continue;
            }
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                if (key.isAcceptable()) {
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    socketChannel.configureBlocking(false);
                    socketChannel.register(selector, SelectionKey.OP_READ);
                }
                if (key.isValid() && key.isReadable()) {
                    requestEventProducer.onData(key);
                }
                iterator.remove();
            }
        }
    }

    void handleReq(SocketChannel channel) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        int len;
        if (channel.isOpen() && (len = channel.read(buf)) > 0) {
            buf.flip();
            byte[] bytes = new byte[1024];
            buf.get(bytes, 0, len);
            ByteBuf byteBuf = Unpooled.buffer(bytes.length);
            byteBuf.writeBytes(bytes);
            List request = Lists.newArrayList();
            redisCommandDecoder.decode(null, byteBuf, request);
            for (Object o : request) {
                reply(channel, o);
            }
        }
    }

    void reply(SocketChannel ctx, Object msg) {
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
                reply = wrapper.execute(msg_, ctx);
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
            ctx.write(byteBuf.nioBuffer());
            byteBuf.release();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    interface Wrapper {

        Reply execute(Command command, SocketChannel ch) throws RedisException;
    }

    public void initRedis(final RedisServer rs) {
        final Class<? extends RedisServer> aClass = rs.getClass();
        for (final Method method : aClass.getMethods()) {
            final Class<?>[] types = method.getParameterTypes();
            methods.put(new BytesKey(method.getName().getBytes()), new Wrapper() {
                @Override
                public Reply execute(Command command, SocketChannel ch) throws RedisException {
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
