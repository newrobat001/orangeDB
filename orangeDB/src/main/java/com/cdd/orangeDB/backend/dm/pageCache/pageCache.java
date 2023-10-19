package com.cdd.orangeDB.backend.dm.pageCache;

import com.cdd.orangeDB.backend.dm.page.Page;
import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.common.ERROR;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface pageCache {
    //定义页大小为8kb，1 << 13 =2^13 2的13次方
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);

    public static PageCacheImpl creat(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(ERROR.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(ERROR.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);

        if (!f.exists()) {
            Panic.panic(ERROR.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(ERROR.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
}
