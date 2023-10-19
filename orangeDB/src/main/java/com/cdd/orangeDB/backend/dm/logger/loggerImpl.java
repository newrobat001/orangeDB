package com.cdd.orangeDB.backend.dm.logger;

import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.backend.utils.Parser;
import com.cdd.orangeDB.common.ERROR;
import com.google.common.primitives.Bytes;

import javax.print.DocFlavor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class loggerImpl implements logger {
    private static final int SEED = 13331;
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    public static final String LOG_SUFFIX = ".log";
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private long position;
    private long fileSize;
    private int xChecksum;


    public loggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public loggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calCheckSum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2byte(xChecksum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2byte(calCheckSum(0, data));
        byte[] size = Parser.int2byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
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

    public void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(ERROR.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position();
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;
        checkAndRemoveTail();
    }

    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calCheckSum(xCheck, log);
        }
        if (xCheck != xChecksum) {
            Panic.panic(ERROR.BadLogFileException);
        }
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    ;

    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp=ByteBuffer.allocate(4);
        try {
            fc.position();
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA > fileSize){
            return null;
        }
        ByteBuffer buf= ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log= buf.array();
        int checkSum1= calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2= Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2){
            return null;
        }
        position += log.length;
        return log;
    }
}
