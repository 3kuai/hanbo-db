package com.lmx.jredis.core.datastruct;

import com.lmx.jredis.storage.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Slf4j
public class SimpleList extends BaseOP {

    int storeSize;

    SimpleList(int storeSize) {
        this.storeSize = storeSize;
    }

    public void init(int db) {
        try {
            store = new DataMedia(db, "listData", storeSize);
        } catch (Exception e) {
            log.error("init store file error", e);
        }
    }

    public boolean write(String key, String value) {
        try {
            if (super.write(key, value)) {
                ByteBuffer b = ByteBuffer.allocateDirect(128);
                int length = value.getBytes().length;
                b.putInt(length);
                b.put(value.getBytes(BaseMedia.CHARSET));
                b.flip();
                DataHelper dh = store.add(b);
                dh.setType(DataTypeEnum.LIST.getDesc());
                dh.setKey(key);
                dh.setLength(length);
                ih.add(dh);
                return true;
            }
        } catch (Exception e) {
            log.error("write list data error", e);
        }
        return false;
    }

    public List<byte[]> read(String key, int startIdx, int endIdx) {
        try {
            if (super.isExpire(key)) {
                return null;
            }
            List<byte[]> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Object l : (List) (ih.kv).get(key)) {
                resp.add(store.get((DataHelper) l));
            }
            resp = resp.subList(startIdx, endIdx == -1 ? resp.size() : endIdx);
            log.debug("key={},value={} cost={}ms", key, resp, (System.currentTimeMillis() - start));
            return resp;
        } catch (Exception e) {
            log.error("read list data error", e);
        }
        return null;
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? ih.type(key) instanceof List : true;
    }

    @Override
    public void removeData(String key) {
        for (DataHelper d : (List<DataHelper>) ih.type(key)) {
            ih.remove(d);
            store.remove(d);
        }
    }
}
