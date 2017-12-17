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
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
//                    log.error("event = [" + event + "], sequence = [" + sequence + "], endOfBatch = [" + endOfBatch + "]");
                    if (event.getValue() instanceof SelectionKey) {
                        SelectionKey key = (SelectionKey) event.getValue();
                        SocketChannel socketChannel = null;
                        try {
                            socketChannel = (SocketChannel) key.channel();
                            handleReq(socketChannel);
                            return;
                        } catch (Exception e) {
                            try {
                                key.cancel();
                                socketChannel.socket().close();
                                socketChannel.close();
                                return;
                            } catch (Exception e1) {
                            }
                        }
                    }
                    if (event.getValue() instanceof Socket) {
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
            });
            RingBuffer ringBuffer = disruptor.start();
            requestEventProducer.setRingBuffer(ringBuffer);
            bindPort();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void bindPort() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startNioServer();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startSocket();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static final byte LOWER_DIFF = 'a' - 'A';
    private Map<BytesKey, Wrapper> methods = new HashMap();

    void startNioServer() throws Exception {
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
                iterator.remove();
                if (key.isValid()) {
                    if (key.isAcceptable()) {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector, SelectionKey.OP_READ);
                    }
                    if (key.isReadable()) {
                        requestEventProducer.onData(key);
                    }
                }
            }
        }
    }

    void startSocket() {
        try {
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress("0.0.0.0", 16381));
            System.err.printf("jRedis bio server listening on port=%d \n", 16381);
            while (true) {
                requestEventProducer.onData(serverSocket.accept());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void handleReq(SocketChannel channel) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        int len;
        if ((len = channel.read(buf)) > 0) {
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

    void handleReq(ByteBuf byteBuf, Socket socket) throws Exception {
        List request = Lists.newArrayList();
        redisCommandDecoder.decode(null, byteBuf, request);
        for (Object o : request) {
            reply(socket, o);
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
