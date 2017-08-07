package com.lmx.xfound.core.datastruct;

import com.lmx.xfound.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 基于内存读写key value操作,数据可持久,零延迟
 * Created by lmx on 2017/4/14.
 */
@Component
@Slf4j
public class SimpleList {
    DataMedia store;
    IndexHelper ih;

    @Value("${memorySize:1024}")
    int storeSize;
    int listSize;

    @PostConstruct
    public void init() {
        try {
            store = new DataMedia("listData", storeSize);
            ih = new IndexHelper("listIndex", storeSize / 8) {
                public void wrapData(DataHelper dataHelper) {
                    if (dataHelper.getType().equals("list")) {
                        if (!kv.containsKey(dataHelper.getKey())) {
                            kv.put(dataHelper.getKey(), new LinkedList<DataHelper>());
                            listSize++;
                        }
                        ((List) kv.get(dataHelper.getKey())).add(dataHelper);
                    }
                }
            };
            ih.recoverIndex();
            log.info("recover data list size: {}", listSize);
        } catch (Exception e) {
            log.error("init store file error", e);
        }
    }

    public int write(String request) {
        try {
            ByteBuffer b = ByteBuffer.allocateDirect(128);
            int length = request.getBytes().length;
            b.putInt(length);
            b.put(request.getBytes("utf8"));
            b.flip();
            DataHelper dh = store.addList(b);
            return ih.add(dh);
        } catch (Exception e) {
            log.error("write list data error", e);
        }
        return -1;
    }

    public List<byte[]> read(String request, int startIdx, int endIdx) {
        try {
            List<byte[]> resp = new ArrayList<>();
            long start = System.currentTimeMillis();
            for (Object l : (List) (ih.kv).get(request)) {
                if (l instanceof DataHelper)
                    resp.add(store.get((DataHelper) l));
            }
            resp = resp.subList(startIdx, endIdx == -1 ? resp.size() : endIdx);
            log.debug("key={},value={} cost={}ms", request, resp, (System.currentTimeMillis() - start));
            return resp;
        } catch (Exception e) {
            log.error("read list data error", e);
        }
        return null;
    }
}
