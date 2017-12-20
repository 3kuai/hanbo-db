package com.lmx.jredis.transport.jsocket;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.lmx.jredis.core.SimpleNioRedisServer;
import com.lmx.jredis.core.datastruct.RedisDbDelegate;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Created by limingxin on 2017/12/14.
 */
@Service
@Slf4j
@Order(2)
public class NioServer {
    @Autowired
    RedisDbDelegate simpleStructDelegate;
    @Autowired
    RequestEventProducer requestEventProducer;
    @Autowired
    NetEventHandler netEventHandler;
    BufferUtil bufferUtil = new BufferUtil();
    SimpleNioRedisServer simpleNioRedisServer = new SimpleNioRedisServer();

    @Data
    static class BufferUtil {
        ByteBuffer readBuf = ByteBuffer.allocate(1024);
        ByteBuf writeBuf = Unpooled.buffer(1024 * 1024 * 20);
    }

//    @PostConstruct
    public void init() {
        try {
            simpleNioRedisServer.initStore(null, simpleStructDelegate);
            netEventHandler.initRedis(simpleNioRedisServer);

            Disruptor<RequestEvent> disruptor = new Disruptor(RequestEvent.FACTORY, 2 << 16, DaemonThreadFactory.INSTANCE);
            disruptor.handleEventsWith(netEventHandler);

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
    }


    void startNioServer() throws Exception {
        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
//        serverSocketChannel.setOption(StandardSocketOptions.SO_SNDBUF, Integer.MAX_VALUE);
        InetSocketAddress address = new InetSocketAddress("0.0.0.0", 16380);
        System.err.printf("jRedis nio server listening on port=%d \n", 16380);
        serverSocketChannel.bind(address);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        while (!Thread.interrupted()) {
            int readyChannels = selector.select();
            if (readyChannels == 0) {
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
                    } else if (key.isReadable()) {
                        simpleNioRedisServer.setKey(key);
                        try {
//                          requestEventProducer.onData(key, selector);
                            netEventHandler.handleReq(key, bufferUtil);
                            if (bufferUtil.getWriteBuf().readableBytes() > 0) {
                                /*SocketChannel socketChannel = (SocketChannel) key.channel();
                                socketChannel.configureBlocking(false);
                                socketChannel.register(selector, SelectionKey.OP_WRITE);*/

                                ByteBuffer resp = bufferUtil.getWriteBuf().nioBuffer();
                                SocketChannel socketChannel = (SocketChannel) key.channel();
                                socketChannel.write(resp);
                                resp.clear();

                                bufferUtil.getWriteBuf().clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            try {
                                key.cancel();
                                key.channel().close();
                            } catch (Exception e1) {
                            }
                        }
                    } /*else if (key.isWritable()) {
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        try {
                            ByteBuffer resp = bufferUtil.getWriteBuf();
                            socketChannel.write(resp);
                            resp.clear();
                            socketChannel.configureBlocking(false);
                            socketChannel.register(selector, SelectionKey.OP_READ);

                        } catch (Exception e) {
                            e.printStackTrace();
                            try {
                                key.cancel();
                                key.channel().close();
                            } catch (Exception e1) {
                            }
                        }
                    }*/
                }
            }
        }
    }

}
