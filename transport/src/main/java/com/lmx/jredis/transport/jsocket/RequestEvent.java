package com.lmx.jredis.transport.jsocket;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

import java.nio.channels.Selector;

/**
 * Created by limingxin on 2017/12/14.
 */
@Data
public class RequestEvent<T> {
    T value;
    Selector selector;
    static EventFactory<RequestEvent> FACTORY = new EventFactory() {
        @Override
        public RequestEvent newInstance() {
            return new RequestEvent();
        }
    };
}
