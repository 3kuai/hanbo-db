package com.lmx.jredis.storage;

import lombok.Data;

/**
 * Created by lmx on 2017/4/17.
 */
@Data
public class DataHelper {
    String type = "kv";
    String key;
    String hash;
    int pos;//value position
    int selfPos;//key position
    int length;//value bytes length
    long expire;//timeout
}
