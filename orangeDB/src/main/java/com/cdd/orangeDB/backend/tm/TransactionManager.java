package com.cdd.orangeDB.backend.tm;


import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.common.ERROR;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author cdd
 */
public interface TransactionManager {
    long begin();

    void commit(long oid);

    void abort(long oid);

    boolean isActive(long oid);

    boolean isCommitted(long oid);

    boolean isAborted(long oid);

    void close();

    /**
     * 创建数据库
     *
     * @param path
     * @return
     */
    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.OID_SUFFIX);
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
        RandomAccessFile ranAf = null;
        try {
            ranAf = new RandomAccessFile(f, "rw");
            fc = ranAf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        //写空OID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_OID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(ranAf, fc);
    }

    /**
     * 打开数据库
     *
     * @param path
     * @return
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.OID_SUFFIX);
        if (!f.exists()) {
            //全局异常处理直接退出
            Panic.panic(ERROR.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            //全局异常处理直接退出
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
        return new TransactionManagerImpl(raf, fc);
    }
}
