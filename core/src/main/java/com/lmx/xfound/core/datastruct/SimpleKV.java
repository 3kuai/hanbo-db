package com.lmx.xfound.core.datastruct;

import com.lmx.xfound.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Component
@Slf4j
public class SimpleKV {
    DataMedia store;
    IndexHelper ih;

    @Value("${memorySize:1024}")
    int storeSize;
    int kvSize;

    @PostConstruct
    public void init() {
        try {
            store = new DataMedia("valueData", storeSize);
            ih = new IndexHelper("keyIndex", storeSize / 8) {
                public void wrapData(DataHelper dataHelper) {
                    if (dataHelper.getType().equals("kv")) {
                        if (!kv.containsKey(dataHelper.getKey())) {
                            kv.put(dataHelper.getKey(), dataHelper);
                            kvSize++;
                        }
                    }
                }
            };
            ih.recoverIndex();
            log.info("recover data kv size: {}", kvSize);
        } catch (Exception e) {
            log.error("init store file error", e);
        }
    }

    public void write(String request) {
        try {
            ByteBuffer b = ByteBuffer.allocateDirect(128);
            int length = request.getBytes().length;
            b.putInt(length);
            b.put(request.getBytes("utf8"));
            b.flip();
            DataHelper dh = store.add(b);
            ih.add(dh);
        } catch (Exception e) {
            log.error("write data error", e);
        }
    }

    public byte[] read(String request) {
        try {
            long start = System.currentTimeMillis();
            byte[] data = store.get((DataHelper) ih.kv.get(request));
            String resp = new String(data, "utf8");
            log.debug("key={},value={} cost={}ms", request, resp, (System.currentTimeMillis() - start));
            return data;
        } catch (Exception e) {
            log.error("read data error", e);
        }
        return null;
    }

    public void remove(String key) {
        ih.kv.remove(key);
        store.remove((DataHelper) ih.kv.get(key));
    }
}
