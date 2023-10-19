package com.cdd.orangeDB.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Parser {
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static byte[] int2byte(int value){
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static byte[] short2Byte(short value){
        return ByteBuffer.allocate(Short.SIZE/ Byte.SIZE)
                .putShort(value).array();
    }

    public static byte[] string2Byte(String str){
        byte[] b=int2byte(str.length()  );
        return Bytes.concat(b, str.getBytes());
    }
    public static long str2Uid(String key){
        long seed= 13331;
        long res= 0;
        for (byte b : key.getBytes()){
            res = res * seed + (long) b;
        }
        return res;
    }
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }
    public static short parseShort(byte[] buf){
        ByteBuffer bf= ByteBuffer.wrap(buf, 0, 2);
        return bf.getShort();
    }
    public static int parseInt(byte[] buf){
        ByteBuffer bf= ByteBuffer.wrap(buf, 0, 4);
        return bf.getInt();
    }

}
