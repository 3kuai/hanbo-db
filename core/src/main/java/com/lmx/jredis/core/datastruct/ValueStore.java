package com.lmx.jredis.core.datastruct;

import com.google.common.base.Charsets;
import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.storage.DataHelper;
import com.lmx.jredis.storage.DataMedia;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Slf4j
public class ValueStore extends AbstractStoreMedia {
    int dataMediaSize;

    ValueStore(int dataMediaSize) {
        this.dataMediaSize = dataMediaSize;
    }

    public void init(int db) {
        try {
            dataMedia = new DataMedia(db, "valueData", dataMediaSize);
        } catch (Exception e) {
            log.error("init dataMedia file error", e);
        }
    }

    public boolean write(String key, String value) {
        try {
            if (super.write(key, value)) {
                DataHelper dataHelper = (DataHelper) indexHelper.type(key);
                if (dataHelper != null) {
                    dataHelper = dataMedia.update(dataHelper, value.getBytes(Charsets.UTF_8));
                    indexHelper.updateIndex(dataHelper);
                    return true;
                } else {
                    ByteBuffer b = ByteBuffer.allocateDirect(128);
                    int length = value.getBytes().length;
                    b.putInt(length);
                    b.put(value.getBytes(Charsets.UTF_8));
                    b.flip();
                    DataHelper dh = dataMedia.add(b);
                    dh.setKey(key);
                    dh.setLength(length);
                    indexHelper.add(dh);
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("write data error", e);
        }
        return false;
    }

    public byte[] read(String key) throws RedisException {
        try {
            if (!checkKeyType(key)) {
                throw new RedisException("Operation against a key holding the wrong kind of value");
            }
            if (super.isExpire(key)) {
                return null;
            }
            long start = System.currentTimeMillis();
            byte[] data = dataMedia.get((DataHelper) indexHelper.type(key));
            String resp = new String(data, Charsets.UTF_8);
            log.debug("key={},value={} cost={}ms", key, resp, (System.currentTimeMillis() - start));
            return data;
        } catch (Exception e) {
            log.error("read data error", e);
            throw e;
        }
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? indexHelper.type(key) instanceof DataHelper : true;
    }

    @Override
    public void removeData(String key) {
        DataHelper dataHelper = (DataHelper) indexHelper.type(key);
        indexHelper.remove(dataHelper);
        dataMedia.remove(dataHelper);
    }
}
