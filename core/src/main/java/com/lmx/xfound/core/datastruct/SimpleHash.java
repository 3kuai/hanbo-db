package com.lmx.xfound.core.datastruct;

import com.lmx.xfound.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Component
@Slf4j
public class SimpleHash extends BaseOP {
    DataMedia store;
    IndexHelper ih;

    @Value("${memorySize:1024}")
    int storeSize;
    int hashSize;

    @PostConstruct
    public void init() {
        try {
            store = new DataMedia("hashData", storeSize);
            ih = new IndexHelper("hashIndex", storeSize / 8) {
                public void wrapData(DataHelper dataHelper) {
                    if (dataHelper.getType().equals("hash")) {
                        if (!kv.containsKey(dataHelper.getHash())) {
                            kv.put(dataHelper.getHash(), new HashMap<String, DataHelper>());
                            hashSize++;
                        }
                        ((Map) kv.get(dataHelper.getHash())).put(dataHelper.getKey(), dataHelper);
                    }
                }
            };
            ih.recoverIndex();
            log.info("recover data hash size: {}", hashSize);
        } catch (Exception e) {
            log.error("init store file error", e);
        }
    }

    public boolean write(String hash, String key, String value) {
        try {
            if (super.write(key, value)) {
                ByteBuffer b = ByteBuffer.allocateDirect(128);
                int hashL = hash.getBytes().length;
                b.putInt(hashL);
                b.put(hash.getBytes("utf8"));
                String request = key + ":" + value;
                int length = request.getBytes().length;
                b.putInt(length);
                b.put(request.getBytes("utf8"));
                b.flip();
                DataHelper dh = store.addHash(b);
                ih.add(dh);
                return true;
            }
        } catch (Exception e) {
            log.error("write list data error", e);
        }
        return false;
    }

    public byte[] read(String hash, String field) {
        try {
            List<String> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Map.Entry<String, DataHelper> e : ((Map<String, DataHelper>) IndexHelper.type(hash)).entrySet()) {
                if (e.getKey().equals(field))
                    return store.get(e.getValue());
            }
            log.debug("key={},value={} cost={}ms", field, resp, (System.currentTimeMillis() - start));
        } catch (Exception e) {
            log.error("read list data error", e);
        }
        return null;
    }

    public byte[][] read(String hash) {
        try {
            byte[][] data = new byte[((Map) IndexHelper.type(hash)).size() * 2][];
            List<String> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            int i = 0;
            for (Map.Entry<String, DataHelper> e : ((Map<String, DataHelper>) IndexHelper.type(hash)).entrySet()) {
                data[i++] = e.getKey().getBytes();
                data[i++] = store.get(e.getValue());
            }
            log.debug("key={},value={} cost={}ms", hash, resp, (System.currentTimeMillis() - start));
            return data;
        } catch (Exception e) {
            log.error("read list data error", e);
        }
        return null;
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? IndexHelper.type(key) instanceof Map : true;
    }

    @Override
    public void removeData(String key) {
        for (DataHelper d : ((Map<String, DataHelper>) IndexHelper.type(key)).values()) {
            store.remove(d);
        }
    }
}
