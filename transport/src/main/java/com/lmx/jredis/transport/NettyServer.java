package com.lmx.jredis.transport;

import com.lmx.jredis.core.PubSubHelper;
import com.lmx.jredis.core.RedisCommandInvoker;
import com.lmx.jredis.core.RedisCommandProcessor;
import com.lmx.jredis.core.RedisCommandProcessorImpl;
import com.lmx.jredis.core.dtype.DatabaseRouter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import redis.RedisCommandDecoder;
import redis.RedisReplyEncoder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
@Order(value = 1)
@Slf4j
public class NettyServer {

    @Value("${server.host:0.0.0.0}")
    private String host;
    @Value("${server.backlog:1024}")
    private int backlog;
    @Value("${server.port_:16990}")
    private int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    @Autowired
    private PubSubHelper busHelper;
    @Autowired
    private NettyServerHandler nettyServerHandler;
    @Autowired
    private DatabaseRouter delegate;
    @Autowired
    private RedisCommandInvoker invoker;

    @PostConstruct
    public void start() throws Exception {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);
        RedisCommandProcessor redis = new RedisCommandProcessorImpl();
        invoker.init(redis);
        redis.initStore(busHelper, delegate, invoker);
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .localAddress(port)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF,2 * 1024)
                .childOption(ChannelOption.SO_RCVBUF,4 * 1024)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 1024 * 1024, 4 * 1024 * 1024))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new RedisCommandDecoder());
                        p.addLast(new RedisReplyEncoder());
                        p.addLast(nettyServerHandler);
                    }
                });
        // Start the server.
        serverBootstrap.bind(host, port).sync();
        System.err.printf("jRedis netty server listening on port=%d \n", port);
    }


    @PreDestroy
    public void stop() {
        log.info("destroy server resources");
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
