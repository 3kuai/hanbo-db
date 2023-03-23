package com.lmx.jredis.core.dtype;

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
                    //申请定长内存空间，用于更新时不需要重新申请空间，缺点是越界后导致后续数据value不准确（换句话说内容最多存储上限是124字节）
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
            DataHelper idx = (DataHelper) indexHelper.type(key);
            if (idx == null) {
                return null;
            }
            byte[] data = dataMedia.get(idx);
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
