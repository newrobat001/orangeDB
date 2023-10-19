package com.cdd.orangeDB.backend.dm.page;

import com.cdd.orangeDB.backend.dm.pageCache.PageCacheImpl;
import sun.jvm.hotspot.oops.Array;
import top.guoziyang.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 第一页 特殊管理
 * VC ： ValidCheck
 *
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCacheImpl.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page){
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    public static void setVcOpen(byte[] raw){
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0 , raw, OF_VC,LEN_VC);
    }

    public static void setVcClose(Page page){
        page.setDirty(true);
        setVcClose(page.getData());
    }

    public static void setVcClose(byte[] raw){
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page page){
        return checkVc(page.getData());
    }
    public static boolean checkVc(byte[] raw){
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
