package com.lmx.jredis.core.dtype;

import com.google.common.base.Charsets;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.storage.BaseMedia;
import com.lmx.jredis.storage.DataHelper;
import com.lmx.jredis.storage.DataMedia;
import com.lmx.jredis.storage.DataTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Slf4j
public class HashStore extends AbstractStoreMedia {

    int dataMediaSize;

    HashStore(int dataMediaSize) {
        this.dataMediaSize = dataMediaSize;
    }

    public void init(int db) {
        try {
            dataMedia = new DataMedia(db, "hashData", dataMediaSize);
        } catch (Exception e) {
            log.error("init dataMedia file error", e);
        }
    }

    void putVal(String hash, String field, String value) throws Exception {
        ByteBuffer b = ByteBuffer.allocateDirect(128);
        byte[] data = value.getBytes(Charsets.UTF_8);
        int length = data.length;
        if (length > 128 - 4) {
            throw new RuntimeException("value最大存储上限是124字节");
        }
        b.putInt(length);
        b.put(data);
        b.flip();
        DataHelper dh = dataMedia.add(b);
        dh.setHash(hash);
        dh.setType(DataTypeEnum.HASH.getDesc());
        dh.setKey(field);
        dh.setLength(length);
        indexHelper.add(dh);
    }

    public boolean write(String hash, String field, String value) {
        try {
            if (super.write(field, value)) {
                Map<String, DataHelper> map = ((Map<String, DataHelper>) indexHelper.type(hash));
                if (!CollectionUtils.isEmpty(map)) {
                    if (map.containsKey(field)) {
                        dataMedia.update(map.get(field), value.getBytes(Charsets.UTF_8));
                        indexHelper.updateIndex(map.get(field));
                        return true;
                    } else {
                        putVal(hash, field, value);
                        return true;
                    }
                }
                putVal(hash, field, value);
                return true;
            }
        } catch (Exception e) {
            log.error("write list data error", e);
        }
        return false;
    }

    public byte[] read(String hash, String field) throws RedisException {
        try {
            if (!checkKeyType(hash)) {
                throw new RedisException("Operation against a key holding the wrong kind of value");
            }
            if (super.isExpire(hash)) {
                return null;
            }
            List<String> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            Object idx = indexHelper.type(hash);
            if (idx == null)
                return null;
            for (Map.Entry<String, DataHelper> e : ((Map<String, DataHelper>) idx).entrySet()) {
                if (e.getKey().equals(field))
                    return dataMedia.get(e.getValue());
            }
            log.debug("key={},value={} cost={}ms", field, resp, (System.currentTimeMillis() - start));
        } catch (Exception e) {
            log.error("read list data error", e);
            throw e;
        }
        return null;
    }

    public byte[][] read(String hash) throws RedisException {
        try {
            if (!checkKeyType(hash)) {
                throw new RedisException("Operation against a key holding the wrong kind of value");
            }
            if (super.isExpire(hash)) {
                return null;
            }
            byte[][] data = new byte[((Map) indexHelper.type(hash)).size() * 2][];
            List<String> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            int i = 0;
            for (Map.Entry<String, DataHelper> e : ((Map<String, DataHelper>) indexHelper.type(hash)).entrySet()) {
                data[i++] = e.getKey().getBytes();
                data[i++] = dataMedia.get(e.getValue());
            }
            log.debug("key={},value={} cost={}ms", hash, resp, (System.currentTimeMillis() - start));
            return data;
        } catch (Exception e) {
            log.error("read list data error", e);
            throw e;
        }
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? indexHelper.type(key) instanceof Map : true;
    }

    @Override
    public void removeData(String key) {
        for (DataHelper d : ((Map<String, DataHelper>) indexHelper.type(key)).values()) {
            indexHelper.remove(d);
            dataMedia.remove(d);
        }
    }
}
