package com.lmx.jredis.transport;

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
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        busHelper.unSubscriber(ctx);
        BlockingQueueHelper.getInstance().remListener(ctx);
        ctx.close();
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        busHelper.unSubscriber(ctx);
        BlockingQueueHelper.getInstance().remListener(ctx);
        ctx.close();
    }
}
