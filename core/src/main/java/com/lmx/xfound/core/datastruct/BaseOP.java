package com.lmx.xfound.core.datastruct;

import com.lmx.xfound.storage.IndexHelper;

/**
 * Created by limingxin on 2017/8/7.
 */
public abstract class BaseOP {

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
