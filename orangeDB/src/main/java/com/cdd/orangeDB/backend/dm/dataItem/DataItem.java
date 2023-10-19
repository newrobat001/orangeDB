package com.cdd.orangeDB.backend.dm.dataItem;

import com.cdd.orangeDB.backend.common.SubArray;
import com.cdd.orangeDB.backend.dm.DataManagerImpl;
import com.cdd.orangeDB.backend.dm.page.Page;
import com.cdd.orangeDB.backend.utils.Parser;
import com.google.common.primitives.Bytes;
import com.cdd.orangeDB.backend.dm.dataItem.DataItemImpl;
import top.guoziyang.mydb.backend.utils.Types;


import java.util.Arrays;


public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long oid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dataManager) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], page, uid, dataManager);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
