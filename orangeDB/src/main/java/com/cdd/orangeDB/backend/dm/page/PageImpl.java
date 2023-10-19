package com.cdd.orangeDB.backend.dm.page;

import com.cdd.orangeDB.backend.dm.pageCache.pageCache;
import com.sun.org.apache.bcel.internal.generic.RETURN;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {
    private int pageNumber;
    private byte[] data;
    private boolean dirty;
    private Lock lock;
    private pageCache pc;

    public PageImpl(int pgno, byte[] initData, pageCache pc) {
        this.pageNumber = pgno;
        this.data = initData;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
