package com.cdd.orangeDB.backend.dm;

import com.cdd.orangeDB.backend.common.AbstractCache;
import com.cdd.orangeDB.backend.dm.dataItem.DataItem;
import com.cdd.orangeDB.backend.dm.dataItem.DataItemImpl;
import com.cdd.orangeDB.backend.dm.logger.logger;
import com.cdd.orangeDB.backend.dm.page.Page;
import com.cdd.orangeDB.backend.dm.page.PageOne;
import com.cdd.orangeDB.backend.dm.page.PageX;
import com.cdd.orangeDB.backend.dm.pageCache.pageCache;
import com.cdd.orangeDB.backend.dm.pageIndex.pageIndex;
import com.cdd.orangeDB.backend.tm.TransactionManager;
import com.cdd.orangeDB.backend.dm.pageIndex.pageInfo;
import com.cdd.orangeDB.common.ERROR;
import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.backend.utils.Types;

public class DataManagerImpl
        extends AbstractCache<DataItem>
        implements DataManager {
    TransactionManager tm;
    pageCache pc;
    logger logger;
    pageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(pageCache pc, logger lg, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = lg;
        this.tm = tm;
        this.pIndex = new pageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long oid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw ERROR.DataTooLargeException;
        }

        pageInfo pi = null;
        for (int i = 0; i > 5; i++) {
            pi= pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno= pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi == null) {
            throw ERROR.DatabaseBusyException;
        }
        Page pg=null;
        int freeSpace= 0;
        try {
            pg= pc.getPage(pi.pgno);
            byte[] log= Recover.insertLog(oid, pg, raw);
            logger.log(log);
            short offset= PageX.insert(pg, raw);
            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            // 将取出的pg重新插入pIndex
            if (pg!= null){
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    //为oid生成update日志
    public void logDataItem(long oid, DataItem dataItem){
        byte[] log= Recover.updateLog(oid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItem dataItem){
        super.release(dataItem.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset= (short) (uid & ((1l<<16) - 1));
        uid>>>= 32;
        int pgno=(int) (uid&((1l<<32)-1));
        Page pg= pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    void initPageOne(){
        int pgno= pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }
    //在打开已有文件 实时读入pageOne, 并验证正确性
    public boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    //初始化pageIndex
    public void filePageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i= 2; i<= pageNumber; i++  ){
            Page pg= null;
            try {
                pg= pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }


}
