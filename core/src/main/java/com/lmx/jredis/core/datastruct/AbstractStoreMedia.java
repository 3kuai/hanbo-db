package com.lmx.jredis.core.datastruct;

import com.lmx.jredis.storage.DataMedia;
import com.lmx.jredis.storage.IndexHelper;
import lombok.Data;

/**
 * Created by limingxin on 2017/8/7.
 */
@Data
public abstract class AbstractStoreMedia {
    protected DataMedia dataMedia;
    protected IndexHelper indexHelper;

    public boolean isExpire(String key) {
        long time = indexHelper.getExpire(key);
        if (time == 0)
            return false;
        if (System.currentTimeMillis() - time > 0) {
            remove(key);
            indexHelper.rmExpire(key);
            return true;
        }
        return false;
    }

    public boolean isExist(String key) {
        return indexHelper.exist(key);
    }

    public abstract boolean checkKeyType(String key);

    public abstract void removeData(String key);

    public void remove(String key) {
        removeData(key);
        indexHelper.getKeyMap().remove(key);
    }

    public boolean write(String key, String value) throws Exception {
        return checkKeyType(key);
    }

    public boolean write(int db, String key, String value) {
        return checkKeyType(key);
    }
}
