package com.lmx.xfound.core.datastruct;

import com.google.common.base.Charsets;
import com.lmx.xfound.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Component
@Slf4j
public class SimpleKV extends BaseOP {
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

    public boolean write(String key, String value) {
        try {
            if (super.write(key, value)) {
                ByteBuffer b = ByteBuffer.allocateDirect(128);
                String request = key + ":" + value;
                int length = request.getBytes().length;
                b.putInt(length);
                b.put(request.getBytes(Charsets.UTF_8));
                b.flip();
                DataHelper dh = store.add(b);
                ih.add(dh);
                return true;
            }
        } catch (Exception e) {
            log.error("write data error", e);
        }
        return false;
    }

    public byte[] read(String request) {
        try {
            long start = System.currentTimeMillis();
            byte[] data = store.get((DataHelper) IndexHelper.type(request));
            String resp = new String(data, Charsets.UTF_8);
            log.debug("key={},value={} cost={}ms", request, resp, (System.currentTimeMillis() - start));
            return data;
        } catch (Exception e) {
            log.error("read data error", e);
        }
        return null;
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? IndexHelper.type(key) instanceof DataHelper : true;
    }

    @Override
    public void removeData(String key) {
        store.remove((DataHelper) IndexHelper.type(key));
    }
}
