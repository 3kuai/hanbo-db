package com.lmx.jredis.transport.jsocket;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.channels.Selector;

/**
 * Created by limingxin on 2017/12/14.
 */
@Slf4j
@Component
public class RequestEventProducer<T> {
    @Setter
    RingBuffer ringBuffer;
    EventTranslatorTwoArg eventTranslatorOneArg = new EventTranslatorTwoArg<RequestEvent, T, Selector>() {
        @Override
        public void translateTo(RequestEvent event, long sequence, T arg0, Selector arg1) {
            event.setValue(arg0);
            event.setSelector(arg1);
//            log.error("event = [" + event + "], sequence = [" + sequence + "], arg0 = [" + arg0 + "]");
        }
    };

    void onData(T... value) {
        ringBuffer.publishEvent(eventTranslatorOneArg, value[0], value[1]);
    }

}
