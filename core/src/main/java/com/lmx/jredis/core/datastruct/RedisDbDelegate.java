package com.lmx.jredis.core.datastruct;

import com.lmx.jredis.storage.DataHelper;
import com.lmx.jredis.storage.DataTypeEnum;
import com.lmx.jredis.storage.IndexHelper;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.validation.constraints.Max;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by limingxin on 2017/12/7.
 */
@Component
@Slf4j
public class RedisDbDelegate {
    @Value("${memorySize:1024}")
    int storeSize;
    @Max(10)
    //db size
    @Value("${dbSize:2}")
    int sharedSize;

    static public Map<Integer, RedisDB> db = new ConcurrentHashMap<>();

    public RedisDB select(int dbInx) {
        return db.get(dbInx);
    }

    @PostConstruct
    public void init() {
        for (int i = 0; i < sharedSize; i++) {
            RedisDB redisDB = new RedisDB();
            redisDB.init(i, storeSize);
        }
    }

    @Data
    public static class RedisDB {
        IndexHelper indexHelper;
        SimpleKV simpleKV;
        SimpleList simpleList;
        SimpleHash simpleHash;
        int lastKvSize;

        @Getter
        public Map<String, Object> kvFilter = new ConcurrentHashMap<>();

        void init(int i, int storeSize) {
            try {
                indexHelper = new IndexHelper(i, "keyIndex", storeSize / 2) {
                    public void wrapData(DataHelper dataHelper) {
                        if (dataHelper.getType().equals(DataTypeEnum.KV.getDesc())) {
                            if (!kv.containsKey(dataHelper.getKey())) {
                                kv.put(dataHelper.getKey(), dataHelper);
                                expire.put(dataHelper.getKey(), dataHelper.getExpire());
                                lastKvSize++;
                            }
                        } else if (dataHelper.getType().equals(DataTypeEnum.LIST.getDesc())) {
                            if (!kv.containsKey(dataHelper.getKey())) {
                                kv.put(dataHelper.getKey(), new LinkedList<DataHelper>());
                                expire.put(dataHelper.getKey(), dataHelper.getExpire());
                                lastKvSize++;
                            }
                            ((List) kv.get(dataHelper.getKey())).add(dataHelper);
                        } else if (dataHelper.getType().equals(DataTypeEnum.HASH.getDesc())) {
                            if (!kv.containsKey(dataHelper.getHash())) {
                                kv.put(dataHelper.getHash(), new HashMap<>());
                                expire.put(dataHelper.getKey(), dataHelper.getExpire());
                                lastKvSize++;
                            }
                            ((Map) kv.get(dataHelper.getHash())).put(dataHelper.getKey(), dataHelper);
                        }
                    }
                };
                indexHelper.recoverIndex();
            } catch (Exception e) {
                e.printStackTrace();
            }


            log.info("db: {},recover data key index size: {}", i, lastKvSize);

            simpleKV = new SimpleKV(storeSize);
            simpleKV.init(i);
            simpleKV.setIh(indexHelper);
            simpleList = new SimpleList(storeSize);
            simpleList.init(i);
            simpleList.setIh(indexHelper);
            simpleHash = new SimpleHash(storeSize);
            simpleHash.init(i);
            simpleHash.setIh(indexHelper);
            if (db.get(i) == null) {
                db.put(i, this);
            }
        }
    }
}
