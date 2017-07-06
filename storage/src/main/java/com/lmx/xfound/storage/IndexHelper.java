package com.lmx.xfound.storage;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 索引存储区
 * 格式：头四位放最新值的postion,其次是数据长度和数据内容
 * Created by lmx on 2017/4/14.
 */
@Slf4j
@EqualsAndHashCode(callSuper = false)
public class IndexHelper extends BaseMedia {
    public static Map<String, DataHelper> kv = new ConcurrentHashMap<>();
    public static Map<String, List<DataHelper>> list = new ConcurrentHashMap<>();
    public static Map<String, Map<String, DataHelper>> hash = new ConcurrentHashMap<>();

    public IndexHelper(String fileName, int size) throws Exception {
        super(fileName, size);
    }

    public int add(DataHelper dh) throws Exception {
        if (dh == null) return -1;
        int indexPos = 0;
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

        if (type.equals("hash")) {
            String h = dh.hash;
            byte[] hb = h.getBytes(CHARSET);
            buffer.putInt(hb.length);
            buffer.put(hb);
        }
        buffer.putInt(pos);
        buffer.putInt(dh.length);

        int curPos = buffer.position();
        buffer.position(0);
        buffer.putInt(curPos);//head 4 byte in last postion
        buffer.rewind();

        kv.put(key, dh);
        if (dh.getType().equals("list")) {
            if (!list.containsKey(key)) {
                list.put(key, new LinkedList<DataHelper>());
            }
            list.get(key).add(dh);
            return list.get(key).size();
        }
        if (dh.getType().equals("hash")) {
            if (!hash.containsKey(dh.getHash())) {
                hash.put(dh.getHash(), new HashMap<String, DataHelper>());
            }
            hash.get(dh.getHash()).put(key, dh);
            return hash.get(dh.getHash()).size();
        }
        return 0;
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
            if (type.equals("hash")) {
                int hashLength = buffer.getInt();
                byte[] hashLengthB = new byte[hashLength];
                buffer.get(hashLengthB);
                hash_ = new String(hashLengthB, CHARSET);
            }
            int dataIndex = buffer.getInt();
            int dataLength = buffer.getInt();
            DataHelper dh = new DataHelper();
            dh.key = key;
            dh.pos = dataIndex;
            dh.length = dataLength;
            dh.type = type;
            if (dh.getType().equals("kv")) {
                if (!kv.containsKey(key)) {
                    kv.put(key, dh);
                }
            }
            if (dh.getType().equals("list")) {
                if (!list.containsKey(key)) {
                    list.put(key, new LinkedList<DataHelper>());
                }
                list.get(key).add(dh);
            }
            if (dh.getType().equals("hash")) {
                if (!hash.containsKey(hash_)) {
                    hash.put(hash_, new HashMap<String, DataHelper>());
                }
                hash.get(hash_).put(key, dh);
            }
        }
        log.info("recover data index size: {}", kv.size());
    }
}
