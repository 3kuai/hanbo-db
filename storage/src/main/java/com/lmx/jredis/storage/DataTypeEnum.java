package com.lmx.jredis.storage;

import lombok.Getter;

/**
 * Created by limingxin on 2017/12/8.
 */
public enum DataTypeEnum {
    KV("kv"), LIST("list"), HASH("hash");

    @Getter
    String desc;

    DataTypeEnum(String desc) {
        this.desc = desc;
    }
}
