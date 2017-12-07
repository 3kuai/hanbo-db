package com.lmx.jredis.storage;

import java.nio.ByteBuffer;

/**
 * 数据(value)存储区
 * 格式：头四位放最新值的position,其次是数据长度和数据内容
 * Created by lmx on 2017/4/13.
 */
public class DataMedia extends BaseMedia {

    public DataMedia(String fileName, int size) throws Exception {
        super(fileName, size);
    }

    public DataMedia(int db, String fileName, int size) throws Exception {
        super(db, fileName, size);
    }

    public DataHelper add(ByteBuffer b) throws Exception {
        int pos = 0;
        if ((pos = buffer.getInt()) != 0)
            buffer.position(pos);
        else
            buffer.position(4);
        buffer.put(b);
        buffer.putChar(NORMAL);
        DataHelper dh = new DataHelper();
        dh.pos = pos == 0 ? 4 + 4 : pos + 4;
        int curPos = buffer.position();
        buffer.position(0);
        buffer.putInt(curPos);//head 4 byte in last postion
        buffer.rewind();
        return dh;
    }

    public byte[] get(DataHelper dh) {
        buffer.position(dh.pos);
        byte[] data = new byte[dh.length];
        buffer.get(data);
        if (buffer.getChar() == DELETE)
            return null;
        buffer.rewind();
        return data;
    }

    public void remove(DataHelper dh) {
        buffer.position(dh.pos + dh.length);
        buffer.putChar(DELETE);
        buffer.rewind();
    }

    public DataHelper update(DataHelper dh, byte[] newBuf) {
        buffer.position(dh.pos - 4);
        int length = newBuf.length;
        if (length > maxUnit)
            throw new RuntimeException("exceed max storage limited exception");
        else {
            buffer.putInt(length);
            buffer.put(newBuf);
            dh.length = length;
            buffer.rewind();
            return dh;
        }
    }
}
