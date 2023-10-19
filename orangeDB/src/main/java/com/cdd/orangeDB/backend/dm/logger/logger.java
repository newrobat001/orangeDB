package com.cdd.orangeDB.backend.dm.logger;

import com.cdd.orangeDB.backend.utils.FileCheck;
import com.cdd.orangeDB.backend.utils.Panic;
import com.cdd.orangeDB.backend.utils.Parser;
import com.cdd.orangeDB.common.ERROR;
import com.sun.tools.javac.comp.Check;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface logger {
    void log(byte[] data);

    void truncate(long x) throws Exception;

    byte[] next();

    void rewind();

    void close();

    public static logger create(String path) {
        File f = new File(path + loggerImpl.LOG_SUFFIX);
        FileCheck.checkFile(f);
        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new loggerImpl(raf, fc, 0);
    }

    public static logger open(String path){
        File f=new File(path+loggerImpl.LOG_SUFFIX);
        FileCheck.checkFile(f);
        FileChannel fc= null;
        RandomAccessFile raf= null;
        try {
            raf= new RandomAccessFile(f, "rw");
            fc= raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        loggerImpl logger=new loggerImpl(raf, fc);
        logger.init();
        return logger;
    }
}
