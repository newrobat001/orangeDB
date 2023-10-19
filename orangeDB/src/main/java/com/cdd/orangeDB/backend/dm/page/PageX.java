package com.cdd.orangeDB.backend.dm.page;

import com.cdd.orangeDB.backend.dm.pageCache.pageCache;
import com.cdd.orangeDB.backend.utils.Parser;

import java.util.Arrays;

/**
 * 普通页
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = pageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[pageCache.PAGE_SIZE];

        return raw;
    }

    private static void setFSO(byte[] raw, short ofData){
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, ofData);
    }
    public static short getFSO(Page page){
        return getFSO(page.getData());
    }

    public static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    //将raw 插入page中 返回插入位置
    public static short insert(Page page, byte[] raw){
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset+ raw.length));
        return offset;
    }

    // 获取页面的空闲大小
    public static int getFreeSpace(Page page){
        return pageCache.PAGE_SIZE- (int) getFSO(page.getData());
    }

    public static void recoverInsert(Page page, byte[] raw, short offset){
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        short rawFSO= getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short)(offset+raw.length) );
        }
    }

    public static void recoverUpdate(Page page, byte[] raw, short offset){
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
}
