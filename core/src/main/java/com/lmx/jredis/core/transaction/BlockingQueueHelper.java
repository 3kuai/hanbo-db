package com.lmx.jredis.core.transaction;

import com.google.common.collect.Lists;
import com.lmx.jredis.core.RedisCommandProcessorImpl;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.datastruct.SimpleList;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.concurrent.DefaultEventExecutor;
import lombok.extern.slf4j.Slf4j;
import redis.netty4.BulkReply;
import redis.netty4.MultiBulkReply;
import redis.netty4.Reply;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2018/12/30.
 */
@Slf4j
public class BlockingQueueHelper {
    private static final BlockingQueueHelper instance = new BlockingQueueHelper();
    private static final Map<String, List<ChannelHandlerContext>> queues = new ConcurrentHashMap<>();
    private static final Map<String, SimpleList> routers = new ConcurrentHashMap<>();
    private static final DefaultEventExecutor defaultEventExecutor = new DefaultEventExecutor();

    public static BlockingQueueHelper getInstance() {
        return instance;
    }

    private BlockingQueueHelper() {
    }

    static {
       /*defaultEventExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (Map.Entry<String, SimpleList> entry : routers.entrySet()) {
                        String k = entry.getKey();
                        if (queues.containsKey(k)) {
                            SimpleList v = entry.getValue();
                            //only once read a element
                            List<byte[]> list_ = v.read(k, 0, 1);
                            if (!CollectionUtils.isEmpty(list_)) {
                                Reply[] replies = new Reply[list_.size()];
                                for (int i = 0; i < replies.length; ++i) {
                                    replies[i] = new BulkReply(list_.get(i));
                                }
                                //random assign a channel to receive data
                                List<ChannelHandlerContext> contextList = queues.get(k);
                                int randomVal = new Random().nextInt(contextList.size());
                                contextList.get(randomVal).writeAndFlush(new MultiBulkReply(replies));
                                contextList.remove(randomVal);
                                if (contextList.size() == 0)
                                    queues.clear();
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1, 5, TimeUnit.SECONDS);*/
    }

    public void notifyListener(String key) {
        if (queues.containsKey(key)) {
            List<ChannelHandlerContext> contextList = queues.get(key);
            SimpleList v = routers.get(key);
            //random assign a channel to receive data
            int randomVal = new Random().nextInt(contextList.size());
            Attribute attribute = contextList.get(randomVal).channel().attr(RedisCommandProcessorImpl.attributeKey);
            int position = (int) attribute.get();
            attribute.set(null);
            byte[] list_ = new byte[0];
            try {
                list_ = v.pop(key, position);
            } catch (RedisException e) {
                log.error("", e);
            }
            if (list_ != null) {
                Reply[] replies = new BulkReply[]{new BulkReply(key.getBytes()), new BulkReply(list_)};
                contextList.get(randomVal).writeAndFlush(new MultiBulkReply(replies));
                contextList.remove(randomVal);
                if (contextList.size() == 0) {
                    queues.clear();
                    routers.clear();
                }
            }
        }
    }

    public void regListener(ChannelHandlerContext channelHandlerContext,
                            String list, SimpleList simpleList) {
        if (!queues.containsKey(list)) {
            queues.put(list, Lists.newArrayList(channelHandlerContext));
            routers.put(list, simpleList);
        } else
            queues.get(list).add(channelHandlerContext);
    }

    public void remListener(ChannelHandlerContext channelHandlerContext) {
        queues.values().remove(channelHandlerContext);
    }

}
