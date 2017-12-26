package com.lmx.jredis.storage;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 索引(key)存储区
 * 格式：头四位放最新值的position,其次是数据长度和数据内容
 * Created by lmx on 2017/4/14.
 */
@Slf4j
@EqualsAndHashCode(callSuper = false)
public abstract class IndexHelper extends BaseMedia {
    static public final int maxCacheLimit = Integer.MAX_VALUE;
    @Getter
    public Map<String, Object> kv = new LRUCache<>(maxCacheLimit);
    public Map<String, Long> expire = new LRUCache<>(maxCacheLimit);

    public IndexHelper(int db, String fileName, int size) throws Exception {
        super(db, fileName, size);
    }

    public Object type(String key) {
        return kv.get(key);
    }

    public void setExpire(String key, long timeOut) {
        expire.put(key, timeOut + System.currentTimeMillis());
    }

    public long getExpire(String key) {
        if (expire.containsKey(key))
            return expire.get(key);
        else
            return 0L;
    }

    public long rmExpire(String key) {
        return expire.remove(key);
    }

    public boolean exist(String key) {
        return kv.containsKey(key);
    }

    public void remove(String key) {
        kv.remove(key);
    }

    public void add(DataHelper dh) throws Exception {
        if (dh == null)
            return;
        int indexPos;
        if ((indexPos = buffer.getInt()) != 0)
            buffer.position(indexPos);
        else
            buffer.position(4);
        String key = dh.key;
        byte[] keyBytes = key.getBytes(CHARSET);
        int pos = dh.pos;

        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        String type = dh.type;
        byte[] typeBytes = type.getBytes(CHARSET);
        buffer.putInt(typeBytes.length);
        buffer.put(typeBytes);
        byte[] hb;
        if (type.equals(DataTypeEnum.HASH.getDesc())) {
            String h = dh.hash;
            hb = h.getBytes(CHARSET);
            buffer.putInt(hb.length);
            buffer.put(hb);
        }
        buffer.putInt(pos);
        buffer.putInt(dh.length);
        buffer.putLong(dh.expire);
        buffer.putChar(NORMAL);

        int curPos = buffer.position();
        buffer.position(0);
        buffer.putInt(curPos);//head 4 byte in last postion
        dh.selfPos = curPos - 2;
        buffer.rewind();
        if (dh.getType().equals(DataTypeEnum.KV.getDesc())) {
            kv.put(key, dh);
        } else if (dh.getType().equals(DataTypeEnum.LIST.getDesc())) {
            if (!kv.containsKey(key)) {
                kv.put(key, new LinkedList<DataHelper>());
            }
            ((List) kv.get(key)).add(dh);
        } else if (dh.getType().equals(DataTypeEnum.HASH.getDesc())) {
            if (!kv.containsKey(dh.getHash())) {
                kv.put(dh.getHash(), new HashMap<>());
            }
            ((Map) kv.get(dh.getHash())).put(key, dh);
        }
    }

    public void updateIndex(DataHelper dh) {
        buffer.position(dh.selfPos - 8 - 4);
        buffer.putInt(dh.length);
        buffer.rewind();
    }

    public void remove(DataHelper dh) {
        buffer.position(dh.selfPos);
        buffer.putChar(DELETE);
        buffer.rewind();
    }

    public void recoverIndex() throws Exception {
        boolean first = true;
        buffer.position(4);
        while (buffer.hasRemaining()) {
            int keyLength = buffer.getInt();
            if (first && keyLength <= 0) {
                first = false;
                buffer.rewind();
                break;
            }
            if (keyLength <= 0)
                break;
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String key = new String(keyBytes, CHARSET);

            int typeLength = buffer.getInt();
            byte[] typeBytes = new byte[typeLength];
            buffer.get(typeBytes);
            String type = new String(typeBytes, CHARSET);
            String hash_ = null;
            if (type.equals(DataTypeEnum.HASH.getDesc())) {
                int hashLength = buffer.getInt();
                byte[] hashLengthB = new byte[hashLength];
                buffer.get(hashLengthB);
                hash_ = new String(hashLengthB, CHARSET);
            }
            int dataIndex = buffer.getInt();
            int dataLength = buffer.getInt();
            long expire = buffer.getLong();
            char status = buffer.getChar();
            DataHelper dh = new DataHelper();
            dh.key = key;
            dh.pos = dataIndex;
            dh.length = dataLength;
            dh.type = type;
            dh.hash = hash_;
            dh.expire = expire;
            dh.selfPos = buffer.position() - 2;
            if (status == NORMAL)
                wrapData(dh);
        }
    }

    public abstract void wrapData(DataHelper dataHelper);
}
