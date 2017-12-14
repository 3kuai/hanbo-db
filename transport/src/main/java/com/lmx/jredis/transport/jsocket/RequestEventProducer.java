package com.lmx.jredis.transport.jsocket;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import lombok.Setter;

/**
 * Created by limingxin on 2017/12/14.
 */
public class RequestEventProducer<T> {
    @Setter
    RingBuffer ringBuffer;
    EventTranslatorOneArg eventTranslatorOneArg = new EventTranslatorOneArg<RequestEvent, T>() {
        @Override
        public void translateTo(RequestEvent event, long sequence, T arg0) {
            event.setValue(arg0);
//            System.out.println("event = [" + event + "], sequence = [" + sequence + "], arg0 = [" + arg0 + "]");
        }
    };

    void onData(T value) {
        ringBuffer.publishEvent(eventTranslatorOneArg, value);
    }

}
