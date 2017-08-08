package com.lmx.jredis.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * 存储单元
 * Created by lmx on 2017/4/14.
 */
public class BaseMedia {
    MappedByteBuffer buffer;
    int size = 1024 * 1024;
    FileChannel fileChannel;
    static File file;
    File f;
    static String BASE_DIR = "data";
    final static String CHARSET = "UTF-8";
    final static String SPLITTER = ":";
    final char NORMAL = '1';
    final char DELETE = '0';

    static {
        file = new File(BASE_DIR);
        if (!file.exists())
            file.mkdir();
    }

    public BaseMedia() {
    }

    public BaseMedia(String fileName, int memSize) throws Exception {
        f = new File(file.getAbsolutePath() + File.separator + fileName);
        if (!f.exists())
            f.createNewFile();
        fileChannel = new RandomAccessFile(f, "rw").getChannel();
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, memSize * size);
    }

    public void clean() throws Exception {
        fileChannel.close();
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
                    cleaner.clean();
                    delFile();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    public void delFile() {
        f.delete();
    }

}
