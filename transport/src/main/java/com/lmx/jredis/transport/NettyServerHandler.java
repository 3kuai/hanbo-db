package com.lmx.jredis.transport;

import com.google.common.base.Stopwatch;
import com.lmx.jredis.core.PubSubHelper;
import com.lmx.jredis.core.RedisCommandInvoker;
import com.lmx.jredis.core.queue.BlockingQueueHelper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.netty4.Command;
import redis.netty4.InlineReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.Reply;

import static redis.netty4.ErrorReply.NYI_REPLY;
import static redis.netty4.StatusReply.QUIT;

@Slf4j
@ChannelHandler.Sharable
@Component
public class NettyServerHandler extends SimpleChannelInboundHandler<Command> {

    @Autowired
    private PubSubHelper busHelper;

    @Autowired
    private RedisCommandInvoker invoker;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        Reply reply = invoker.handlerEvent(ctx, msg);
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
            if (reply instanceof MultiBulkReply) {
                MultiBulkReply multiBulkReply = (MultiBulkReply) reply;
                if (multiBulkReply == MultiBulkReply.BLOCKING_QUEUE) {
                    return;
                }
            }
            ctx.write(reply);
            stopwatch.stop();
            if (stopwatch.elapsedMillis() > 1)
                log.error("process cost {}ms", stopwatch.elapsedMillis());
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        releaseConn(ctx);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("connect error: {}", ctx.channel(), cause);
        releaseConn(ctx);
    }

    void releaseConn(ChannelHandlerContext ctx) {
        busHelper.unSubscriber(ctx);
        BlockingQueueHelper.getInstance().remListener(ctx);
        ctx.close();
        ctx.channel().close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        boolean isWrite = ctx.channel().isWritable();
        //false if the out buffer size > higherWaterMarker
        if (!isWrite)
            System.err.println("warning! send buffer is full, wait to free...");
    }
}
