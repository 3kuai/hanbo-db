package com.lmx.jredis.storage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 让不活跃的key自动清除
 * Created by limingxin on 2017/12/26.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int MAX_CACHE_SIZE;

    public LRUCache(int cacheSize) {
        super(16, 0.95f, true);
        MAX_CACHE_SIZE = cacheSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > MAX_CACHE_SIZE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K, V> entry : entrySet()) {
            sb.append(String.format("%s:%s ", entry.getKey(), entry.getValue())).append(",");
        }
        return sb.toString();
    }
}