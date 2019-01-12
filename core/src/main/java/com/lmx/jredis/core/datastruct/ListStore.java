package com.lmx.jredis.core.datastruct;

import com.lmx.jredis.core.RedisException;
import com.lmx.jredis.core.queue.BlockingQueueHelper;
import com.lmx.jredis.storage.BaseMedia;
import com.lmx.jredis.storage.DataHelper;
import com.lmx.jredis.storage.DataMedia;
import com.lmx.jredis.storage.DataTypeEnum;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Slf4j
public class ListStore extends AbstractStoreMedia {

    int dataMediaSize;

    ListStore(int dataMediaSize) {
        this.dataMediaSize = dataMediaSize;
    }

    public void init(int db) {
        try {
            dataMedia = new DataMedia(db, "listData", dataMediaSize);
        } catch (Exception e) {
            log.error("init dataMedia file error", e);
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
                DataHelper dh = dataMedia.add(b);
                dh.setType(DataTypeEnum.LIST.getDesc());
                dh.setKey(key);
                dh.setLength(length);
                indexHelper.add(dh);
                BlockingQueueHelper.getInstance().notifyListener(key);
                return true;
            }
        } catch (Exception e) {
            log.error("write list data error", e);
        }
        return false;
    }

    public List<byte[]> read(String key, int startIdx, int endIdx) throws RedisException {
        try {
            if (!checkKeyType(key)) {
                throw new RedisException("Operation against a key holding the wrong kind of value");
            }
            if (super.isExpire(key)) {
                return null;
            }
            List<byte[]> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Object l : (List) (indexHelper.getKeyMap()).get(key)) {
                resp.add(dataMedia.get((DataHelper) l));
            }
            resp = resp.subList(startIdx, endIdx == -1 ? resp.size() : endIdx+1);
            log.debug("key={},value={} cost={}ms", key, resp, (System.currentTimeMillis() - start));
            return resp;
        } catch (Exception e) {
            log.error("read list data error", e);
            throw e;
        }
    }

    @Override
    public boolean checkKeyType(String key) {
        return isExist(key) ? indexHelper.type(key) instanceof List : true;
    }

    @Override
    public void removeData(String key) {
        for (DataHelper d : (List<DataHelper>) indexHelper.type(key)) {
            indexHelper.remove(d);
            dataMedia.remove(d);
        }
    }

    /**
     * pop the head element in queue
     *
     * @param key
     */
    public byte[] popHead(String key) throws RedisException {
        return pop(key, 1);
    }

    /**
     * pop the tail element in queue
     *
     * @param key
     */
    public byte[] popTail(String key) throws RedisException {
        return pop(key, 0);
    }

    public byte[] pop(String key, int point) throws RedisException {
        if (!checkKeyType(key)) {
            throw new RedisException("Operation against a key holding the wrong kind of value");
        }
        List<DataHelper> list = (List<DataHelper>) indexHelper.type(key);
        DataHelper headData = null;
        if (list.size() > 0) {
            if (point == 0)
                headData = list.remove(list.size() - 1);
            else if (point == 1)
                headData = list.remove(0);
        }
        byte[] val = dataMedia.get(headData);
        dataMedia.remove(headData);
        indexHelper.remove(headData);
        return val;
    }
}
