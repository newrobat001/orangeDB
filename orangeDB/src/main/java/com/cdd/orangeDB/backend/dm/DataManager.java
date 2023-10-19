package com.cdd.orangeDB.backend.dm;

import com.cdd.orangeDB.backend.dm.dataItem.DataItem;
import com.cdd.orangeDB.backend.dm.page.PageOne;
import com.cdd.orangeDB.backend.dm.pageCache.pageCache;
import com.cdd.orangeDB.backend.tm.TransactionManager;
import com.cdd.orangeDB.backend.dm.logger.logger;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long oid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm){
        pageCache pc = pageCache.creat(path, mem);
        logger lg= logger.create(path);
        DataManagerImpl dm=new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }
    public static DataManager open(String path, long mem, TransactionManager tm){
        pageCache pc= pageCache.open(path, mem);
        logger lg=logger.open(path);
        DataManagerImpl dm=new DataManagerImpl(pc, lg, tm);
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.filePageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }
}
