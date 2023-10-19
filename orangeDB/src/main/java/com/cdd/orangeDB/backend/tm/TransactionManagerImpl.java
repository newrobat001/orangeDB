package com.cdd.orangeDB.backend.tm;

import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.common.ERROR;
import com.cdd.orangeDB.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {
    //头文件长度
    static final int LEN_OID_HEADER_LENGTH = 8;
    // 事务占用长度
    private static final int OID_FIELD_SIZE = 1;
    // 事务三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    // 超级事务 永远为committed状态
    public static final long SUPER_OID = 0;
    static final String OID_SUFFIX = ".oid";
    private RandomAccessFile file;
    private FileChannel fc;
    private long oidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile ranAf, FileChannel fc) {
        this.file = ranAf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkOIDCounter();
    }

    //校验文件是否正确
    private void checkOIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(ERROR.BadOIDFileException);
        }
        if (fileLen < LEN_OID_HEADER_LENGTH) {
            Panic.panic(ERROR.BadOIDFileException);
        }
        ByteBuffer buffer = ByteBuffer.allocate(LEN_OID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.oidCounter = Parser.parseLong(buffer.array());
        long end = getOidPosition(this.oidCounter + 1);
        if (end != fileLen) {
            Panic.panic(ERROR.BadOIDFileException);
        }
    }

    // 根据事务oid取得其在oid文件中对应的位置
    private long getOidPosition(long oid) {
        return LEN_OID_HEADER_LENGTH + (oid - 1) * OID_FIELD_SIZE;
    }

    // 更新oid事务状态为status
    private void updateOID(long oid, byte status) {
        long offset = getOidPosition(oid);
        byte[] tmp = new byte[OID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    //oid+1 更新 oid header
    private void incrOIDCounter() {
        oidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(oidCounter));

        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long oid = oidCounter + 1;
            updateOID(oid, FIELD_TRAN_ACTIVE);
            incrOIDCounter();
            return oid;
        } finally {
            counterLock.unlock();
        }

    }

    //
    private boolean checkOID(long oid, byte status) {
        long offset= getOidPosition(oid);
        ByteBuffer wrap = ByteBuffer.wrap(new byte[OID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(wrap);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return wrap.array()[0]==status;

    }

    @Override
    public void commit(long oid) {
        updateOID(oid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long oid) {
        updateOID(oid,FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long oid) {
        if (idSUPER(oid)) return true;
        return checkOID(oid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long oid) {
        if (idSUPER(oid)) return true;
        return checkOID(oid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long oid) {
        if (idSUPER(oid)) return true;
        return checkOID(oid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    //简单判断是否是超级事务
    public static boolean idSUPER(long oid){
        if (oid == SUPER_OID)
            return true;
        return false;
    }
}
