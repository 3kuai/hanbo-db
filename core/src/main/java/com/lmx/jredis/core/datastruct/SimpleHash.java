package com.lmx.jredis.core.datastruct;

import com.google.common.base.Charsets;
import com.lmx.jredis.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Slf4j
public class SimpleHash extends BaseOP {

    int storeSize;

    SimpleHash(int storeSize) {
        this.storeSize = storeSize;
    }

    public void init(int db) {
        try {
            store = new DataMedia(db, "hashData", storeSize);
        } catch (Exception e) {
            log.error("init store file error", e);
        }
    }

    public boolean write(String hash, String field, String value) {
        try {
            if (super.write(field, value)) {
                Map<String, DataHelper> map = ((Map<String, DataHelper>) ih.type(hash));
                if (!CollectionUtils.isEmpty(map)) {
                    if (map.containsKey(field)) {
                        store.update(map.get(field), value.getBytes(Charsets.UTF_8));
                        ih.updateIndex(map.get(field));
                        return true;
                    } else {
                        ByteBuffer b = ByteBuffer.allocateDirect(128);
                        int length = value.getBytes().length;
                        b.putInt(length);
                        b.put(value.getBytes(BaseMedia.CHARSET));
                        b.flip();
                        DataHelper dh = store.add(b);
                        dh.setHash(hash);
                        dh.setType(DataTypeEnum.HASH.getDesc());
                        dh.setKey(field);
                        dh.setLength(length);
                        ih.add(dh);
                        return true;
                    }
                }
                ByteBuffer b = ByteBuffer.allocateDirect(128);
                int length = value.getBytes().length;
                b.putInt(length);
                b.put(value.getBytes(BaseMedia.CHARSET));
                b.flip();
                DataHelper dh = store.add(b);
                dh.setHash(hash);
                dh.setType(DataTypeEnum.HASH.getDesc());
                dh.setKey(field);
                dh.setLength(length);
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
            if (super.isExpire(hash)) {
                return null;
            }
            List<String> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Map.Entry<String, DataHelper> e : ((Map<String, DataHelper>) ih.type(hash)).entrySet()) {
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
            if (super.isExpire(hash)) {
                return null;
            }
            byte[][] data = new byte[((Map) ih.type(hash)).size() * 2][];
            List<String> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            int i = 0;
            for (Map.Entry<String, DataHelper> e : ((Map<String, DataHelper>) ih.type(hash)).entrySet()) {
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
        return isExist(key) ? ih.type(key) instanceof Map : true;
    }

    @Override
    public void removeData(String key) {
        for (DataHelper d : ((Map<String, DataHelper>) ih.type(key)).values()) {
            ih.remove(d);
            store.remove(d);
        }
    }
}
