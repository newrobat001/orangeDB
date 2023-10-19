package com.cdd.orangeDB.backend.dm.pageCache;

import com.cdd.orangeDB.backend.common.AbstractCache;
import com.cdd.orangeDB.backend.dm.page.Page;
import com.cdd.orangeDB.backend.dm.page.PageImpl;
import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.common.ERROR;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl
        extends AbstractCache<Page>
        implements pageCache {
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(ERROR.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }


    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        return 0;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();

        return new PageImpl(pgno, buf.array(), this);
    }
    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    @Override
    public void releaseForCache(Page page) {
        if (page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }

    private void flush(Page page) {
        int pgno = page.getPageNumber();
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
}
