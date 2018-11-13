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
    static File defaultFile;
    File f;
    final static String BASE_DIR = "data";
    final public static String CHARSET = "UTF-8";
    final char NORMAL = '1';
    final char DELETE = '0';
    int maxUnit = 1024;
    static int dbLength = 16;

    static {
        file = new File(BASE_DIR);
        if (!file.exists())
            file.mkdir();
        for (int i = 0; i < dbLength; i++) {
            file = new File(BASE_DIR + File.separator + i);
            if (!file.exists())
                file.mkdir();
            if (i == 0) {
                defaultFile = file;
            }
        }

    }

    public BaseMedia() {
    }

    /**
     * 初始化10个分区为db
     *
     * @param db
     * @param fileName
     * @param memSize
     * @throws Exception
     */
    public BaseMedia(int db, String fileName, int memSize) throws Exception {
        if (db == 0)
            f = new File(defaultFile.getAbsolutePath() + File.separator + fileName);
        else
            f = new File(defaultFile.getParentFile().getAbsolutePath() + File.separator + db + File.separator + fileName);
        if (!f.exists())
            f.createNewFile();
        fileChannel = new RandomAccessFile(f, "rw").getChannel();
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, memSize * size);
    }

    public BaseMedia(String fileName, int memSize) throws Exception {
        f = new File(defaultFile.getAbsolutePath() + File.separator + fileName);
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
