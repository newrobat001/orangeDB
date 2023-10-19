package com.cdd.orangeDB.backend.dm.pageIndex;

import com.cdd.orangeDB.backend.dm.pageCache.pageCache;
import com.cdd.orangeDB.backend.dm.pageIndex.pageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class pageIndex {
    //将页化成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = pageCache.PAGE_SIZE / INTERVALS_NO;
    private Lock lock;
    private List<pageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public pageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new pageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public pageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
