package com.cdd.orangeDB.backend.dm;

import com.cdd.orangeDB.backend.common.SubArray;
import com.cdd.orangeDB.backend.dm.logger.logger;
import com.cdd.orangeDB.backend.dm.page.Page;
import com.cdd.orangeDB.backend.dm.page.PageX;
import com.cdd.orangeDB.backend.tm.TransactionManager;
import com.cdd.orangeDB.backend.dm.pageCache.pageCache;
import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.backend.utils.Parser;
import com.google.common.primitives.Bytes;
import jdk.vm.ci.amd64.AMD64;
import sun.jvm.hotspot.interpreter.BytecodeRet;
import com.cdd.orangeDB.backend.dm.dataItem.DataItem;

import java.security.cert.CertPathChecker;
import java.util.*;
import java.util.Map.Entry;

public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long oid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long oid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;

    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    public static void recover(TransactionManager tm, logger lg, pageCache pageCache) {
        System.out.println("Recovering......");

        lg.rewind();
        int maxPgno = 0;
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                pgno = logInfo.pgno;
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pgno = updateLogInfo.pgno;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }
        pageCache.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pageCache);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pageCache);
        System.out.println("undo Transactions Over.");

        System.out.println(" Recover Over");
    }

    private static void redoTranscations(TransactionManager tm, logger lg, pageCache pageCache) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                long oid = logInfo.oid;
                if (!tm.isActive(oid)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long oid = updateLogInfo.oid;
                if (!tm.isActive(oid)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    private static void undoTranscations(TransactionManager tm, logger lg, pageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long oid = li.oid;
                if (tm.isActive(oid)) {
                    if (!logCache.containsKey(oid)) {
                        logCache.put(oid, new ArrayList<>());
                    }
                    logCache.get(oid).add(log);
                }
            } else {
                UpdateLogInfo logInfo = parseUpdateLog(log);
                long oid = logInfo.oid;
                if (tm.isActive(oid)) {
                    if (!logCache.containsKey(oid)) {
                        logCache.put(oid, new ArrayList<>());
                    }
                    logCache.get(oid).add(log);
                }
            }
        }

        //对所有active log 进行倒序undo
        for (Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pageCache, log, UNDO);
                } else {
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    //格式：[LogType] [OID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_OID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_OID + 8;

    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    private static void doUpdateLog(pageCache pc, byte[] log, int flag) {
        int pgno;
        short offset = 0;
        byte[] raw;
        if (flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    private static void doInsertLog(pageCache pageCache, byte[] log, int flag) {
        InsertLogInfo li= parseInsertLog(log);
        Page pg= null;
        try {
            pg= pageCache.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        }finally {
            pg.release();
        }
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.oid = Parser.parseLong(Arrays.copyOfRange(log, OF_OID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short) (uid & ((1l << 16) - 1));
        uid >>>= 32;
        li.pgno = (int) (uid & ((1l << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li= new InsertLogInfo();
        li.oid= Parser.parseLong(Arrays.copyOfRange(log, OF_OID, OF_INSERT_PGNO));
        li.offset= Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    public static byte[] updateLog(long oid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] oidRaw = Parser.long2Byte(oid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, oidRaw, uidRaw, oldRaw, newRaw);
    }

    private static final int OF_INSERT_PGNO = OF_OID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;
    public static byte[] insertLog(long oid, Page pg, byte[] raw) {
        byte[] logTypeRaw={LOG_TYPE_INSERT};
        byte[] oidRaw= Parser.long2Byte(oid);
        byte[] pgnoRaw= Parser.int2byte(pg.getPageNumber());
        byte[] offsetRaw= Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, oidRaw, pgnoRaw, offsetRaw, raw);
    }


}
