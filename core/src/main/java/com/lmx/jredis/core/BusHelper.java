package com.lmx.jredis.core;

import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import redis.netty4.BulkReply;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 基于topic路由
 * Created by Administrator on 2017/4/15.
 */
@Slf4j
@Component
public class BusHelper {
    Map<String, ConcurrentHashMap<ChannelHandlerContext, String>> subscribers = new ConcurrentHashMap<>();
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
            if (!subscribers.containsKey(new String(t))) {
                subscribers.put(new String(t), new ConcurrentHashMap<ChannelHandlerContext, String>());
            }
            subscribers.get(new String(t)).put(channel, "");
            log.info("register subscriber {}", channel.channel().toString());
        }
    }

    public void unSubscriber(ChannelHandlerContext context) {
        for (ConcurrentHashMap<ChannelHandlerContext, String> chcs : subscribers.values()) {
            if (chcs.containsKey(context)) {
                log.info("unSubscriber channel {}", context);
                chcs.remove(context);
            }
        }
    }

    public void pubMsg(Message m) {
        log.info("pub a event {}", m.toString());
        messages.add(m);
    }

    Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
            while (true) {
                try {
                    Message me = messages.take();
                    byte[] topic = me.getTopic();
                    ConcurrentHashMap<ChannelHandlerContext, String> chanList = subscribers.get(new String(topic));
                    if (chanList == null)
                        continue;
                    for (ChannelHandlerContext chc : chanList.keySet()) {
                        if (chc.channel().isOpen()) {
                            log.info("notify channel: {} ,msg: {}", chc.toString(), me.getMsg());
                            chc.channel().writeAndFlush(new BulkReply(me.getMsg()));
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
