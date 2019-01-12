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
public class DatabaseRouter {
    @Value("${memorySize:1024}")
    private int storeSize;
    @Max(10)
    //dbMap size
    @Value("${dbMapSize:2}")
    private int shardSize;

    @Getter
    private Map<Integer, RedisDB> dbMap = new ConcurrentHashMap<>();

    public RedisDB select(int dbMapInx) {
        return dbMap.get(dbMapInx);
    }

    @PostConstruct
    public void init() {
        for (int i = 0; i < shardSize; i++) {
            RedisDB redisDB = new RedisDB();
            redisDB.init(i, storeSize);
        }
    }

    @Data
    public class RedisDB {
        private IndexHelper indexHelper;
        private ValueStore simpleKV;
        private ListStore simpleList;
        private HashStore simpleHash;
        private int lastKvSize;
        private int dbMapIdx;

        @Getter
        public Map<String, Object> keyMapFilter = new ConcurrentHashMap<>();

        void init(int i, int storeSize) {
            try {
                this.dbMapIdx = i;
                indexHelper = new IndexHelper(i, "keyIndex", storeSize / 2) {
                    public void wrapData(DataHelper dataHelper) {
                        if (dataHelper.getType().equals(DataTypeEnum.KV.getDesc())) {
                            if (!keyMap.containsKey(dataHelper.getKey())) {
                                keyMap.put(dataHelper.getKey(), dataHelper);
                                expireMap.put(dataHelper.getKey(), dataHelper.getExpire());
                                lastKvSize++;
                            }
                        } else if (dataHelper.getType().equals(DataTypeEnum.LIST.getDesc())) {
                            if (!keyMap.containsKey(dataHelper.getKey())) {
                                keyMap.put(dataHelper.getKey(), new LinkedList<DataHelper>());
                                expireMap.put(dataHelper.getKey(), dataHelper.getExpire());
                                lastKvSize++;
                            }
                            ((List) keyMap.get(dataHelper.getKey())).add(dataHelper);
                        } else if (dataHelper.getType().equals(DataTypeEnum.HASH.getDesc())) {
                            if (!keyMap.containsKey(dataHelper.getHash())) {
                                keyMap.put(dataHelper.getHash(), new HashMap<>());
                                expireMap.put(dataHelper.getKey(), dataHelper.getExpire());
                                lastKvSize++;
                            }
                            ((Map) keyMap.get(dataHelper.getHash())).put(dataHelper.getKey(), dataHelper);
                        }
                    }
                };
                indexHelper.recoverIndex();
            } catch (Exception e) {
                log.error("", e);
            }

            log.info("dbMap: {},recover data key index size: {}", i, lastKvSize);

            simpleKV = new ValueStore(storeSize);
            simpleKV.init(i);
            simpleKV.setIndexHelper(indexHelper);
            simpleList = new ListStore(storeSize);
            simpleList.init(i);
            simpleList.setIndexHelper(indexHelper);
            simpleHash = new HashStore(storeSize);
            simpleHash.init(i);
            simpleHash.setIndexHelper(indexHelper);
            if (dbMap.get(i) == null) {
                dbMap.put(i, this);
            }
        }

        public void flush() {
            try {
                //release file channel and clean buffer
                indexHelper.getKeyMap().clear();
                indexHelper.getExpireMap().clear();
                indexHelper.clean();
                simpleKV.dataMedia.clean();
                simpleList.dataMedia.clean();
                simpleHash.dataMedia.clean();
                //reInit file
                this.init(dbMapIdx, storeSize);
            } catch (Exception e) {
                log.error("", e);
            }
        }
    }
}
