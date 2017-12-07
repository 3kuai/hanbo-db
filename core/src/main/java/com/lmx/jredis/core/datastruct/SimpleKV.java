package com.lmx.jredis.core.datastruct;

import com.google.common.base.Charsets;
import com.lmx.jredis.storage.*;
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
public class SimpleKV extends BaseOP {
    DataMedia store;
    IndexHelper ih;

    @Value("${memorySize:1024}")
    int storeSize;
    int kvSize;

    @PostConstruct
    public void init() {
        try {
            store = new DataMedia(0, "valueData", storeSize);
            ih = new IndexHelper(0, "keyIndex", storeSize / 8) {
                public void wrapData(DataHelper dataHelper) {
                    if (dataHelper.getType().equals("kv")) {
                        if (!kv.containsKey(dataHelper.getKey())) {
                            kv.put(dataHelper.getKey(), dataHelper);
                            expire.put(dataHelper.getKey(), dataHelper.getExpire());
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

    public boolean write(int db, String key, String value) {
        try {
            if (super.write(key, value)) {
                DataHelper dataHelper = (DataHelper) IndexHelper.type(key);
                if (dataHelper != null) {
                    store = (DataMedia) SimpleDatabase.DBS.get(db).get("valueData");
                    dataHelper = store.update(dataHelper, value.getBytes(Charsets.UTF_8));
                    ih = (IndexHelper) SimpleDatabase.DBS.get(db).get("keyIndex");
                    ih.updateIndex(dataHelper);
                    return true;
                } else {
                    ByteBuffer b = ByteBuffer.allocateDirect(128);
                    int length = value.getBytes().length;
                    b.putInt(length);
                    b.put(value.getBytes(Charsets.UTF_8));
                    b.flip();
                    store = (DataMedia) SimpleDatabase.DBS.get(db).get("valueData");
                    DataHelper dh = store.add(b);
                    dh.setKey(key);
                    dh.setLength(length);
                    ih = (IndexHelper) SimpleDatabase.DBS.get(db).get("keyIndex");
                    ih.add(dh);
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("write data error", e);
        }
        return false;
    }

    public byte[] read(int db, String key) {
        try {
            if (super.isExpire(key)) {
                return null;
            }
            long start = System.currentTimeMillis();
            store = (DataMedia) SimpleDatabase.DBS.get(db).get("valueData");
            ih = (IndexHelper) SimpleDatabase.DBS.get(db).get("keyIndex");
            byte[] data = store.get((DataHelper) ih.type(key));
            String resp = new String(data, Charsets.UTF_8);
            log.debug("key={},value={} cost={}ms", key, resp, (System.currentTimeMillis() - start));
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
        DataHelper dataHelper = (DataHelper) IndexHelper.type(key);
        ih.remove(dataHelper);
        store.remove(dataHelper);
    }
}
