package com.lmx.jredis.core.datastruct;

import com.lmx.jredis.storage.IndexHelper;

/**
 * Created by limingxin on 2017/8/7.
 */
public abstract class BaseOP {

    public boolean isExpire(String key) {
        long time = IndexHelper.getExpire(key);
        if (time == 0)
            return false;
        if (System.currentTimeMillis() - time > 0) {
            remove(key);
            IndexHelper.rmExpire(key);
            return true;
        }
        return false;
    }

    public boolean isExist(String key) {
        return IndexHelper.exist(key);
    }

    public abstract boolean checkKeyType(String key);

    public abstract void removeData(String key);

    public void remove(String key) {
        removeData(key);
        IndexHelper.kv.remove(key);
    }

    public boolean write(String key, String value) {
        return checkKeyType(key);
    }
}
