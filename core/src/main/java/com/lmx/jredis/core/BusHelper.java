package com.lmx.jredis.core;

import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import redis.netty4.BulkReply;
import redis.util.BytesKey;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 基于topic路由
 * Created by Administrator on 2017/4/15.
 */
@Slf4j
@Component
public class BusHelper {
    Map<BytesKey, ConcurrentHashMap<ChannelHandlerContext, BytesKey>> subscribers = new ConcurrentHashMap<>();
    BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static public class Message {
        byte[] topic, msg;

        public String toString() {
            return new String(topic) + ":" + new String(msg);
        }
    }

    public void regSubscriber(ChannelHandlerContext channel, byte[]... topic) {
        for (byte[] t : topic) {
            if (!subscribers.containsKey(new BytesKey(t))) {
                subscribers.put(new BytesKey(t), new ConcurrentHashMap<ChannelHandlerContext, BytesKey>());
            }
            subscribers.get(new BytesKey(t)).put(channel, new BytesKey("".getBytes()));
            log.info("register subscriber {}", channel.channel().toString());
        }
    }

    public void unSubscriber(ChannelHandlerContext context) {
        for (ConcurrentHashMap<ChannelHandlerContext, BytesKey> concurrentHashMap : subscribers.values()) {
            if (concurrentHashMap.containsKey(context)) {
                log.info("unSubscriber channel {}", context);
                concurrentHashMap.remove(context);
            }
        }
    }

    public void pubMsg(Message m) {
        log.info("pub a event {}", m.toString());
        messages.add(m);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
            while (true) {
                try {
                    final Message me = messages.take();
                    byte[] topic = me.getTopic();
                    ConcurrentHashMap<ChannelHandlerContext, BytesKey> chanList = subscribers.get(new BytesKey(topic));
                    if (chanList == null)
                        continue;
                    for (final ChannelHandlerContext chc : chanList.keySet()) {
                        if (chc.channel().isOpen()) {
                            log.info("notify channel: {} ,msg: {}", chc.toString(), me.getMsg());
                            executorService.execute(new Runnable() {
                                @Override
                                public void run() {
                                    chc.channel().writeAndFlush(new BulkReply(me.getMsg()));
                                }
                            });
                        } else {
                            chanList.remove(chc);
                        }
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }
    });

    @PostConstruct
    public void init() {
        t.start();
    }
}
