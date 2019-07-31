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
    final static String BASE_DIR = System.getProperty("data.path") == null ? "data" : System.getProperty("data.path") + File.separator + "data";
    final public static String CHARSET = "UTF-8";
    final char NORMAL = '1';
    final char DELETE = '0';
    int maxUnit = 1024;
    static int dbLength = 8;
    int accessIndex = 0;

    static {
        initFileSys();
    }

    public void setIndex(int i) {
        if (i <= dbLength - 1)
            this.accessIndex = i;
        else
            throw new IndexOutOfBoundsException();
    }

    public static void initFileSys() {
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

    public static void delFileSys() {
        file = new File(BASE_DIR);
        if (file.exists()) {
            for (int i = 0; i < dbLength; i++) {
                file = new File(BASE_DIR + File.separator + i);
                if (file.exists()) {
                    for (File file1 : file.listFiles()) {
                        file1.delete();
                    }
                }
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
        long fileSize = Math.max(memSize * size, fileChannel.size());
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize == 0 ? memSize * size : fileSize);
    }

    public BaseMedia(String fileName, int memSize) throws Exception {
        f = new File(defaultFile.getAbsolutePath() + File.separator + fileName);
        if (!f.exists())
            f.createNewFile();
        fileChannel = new RandomAccessFile(f, "rw").getChannel();
        long fileSize = Math.max(memSize * size, fileChannel.size());
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize == 0 ? memSize * size : fileSize);
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

    public void reAllocate() throws Exception {
        System.err.println("reAllocate file begin");
        String dir = f.getParentFile().getParentFile().getAbsolutePath();

        File newFile = new File(dir + File.separator + accessIndex + File.separator + f.getName() + "_tmp");
        com.google.common.io.Files.copy(f, newFile);

        File newFile_ = new File(f.getAbsolutePath());
        clean();
        com.google.common.io.Files.copy(newFile, newFile_);
        newFile.delete();

        fileChannel = new RandomAccessFile(newFile_, "rw").getChannel();
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, buffer.capacity() * 2);
        System.err.println("reAllocate file end");
    }

    public void resize(int pos) throws Exception {
        //reallocate buffer
        if (buffer.remaining() - pos < 4) {
            reAllocate();
            if ((pos = buffer.getInt()) != 0)
                buffer.position(pos);
            else
                buffer.position(4);
        }
    }

}
