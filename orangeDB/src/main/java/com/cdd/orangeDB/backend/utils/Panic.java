package com.cdd.orangeDB.backend.utils;

public class Panic {
    /**
     * 强制中断退出
     * @param err
     */
    public static void panic (Exception err){
        err.printStackTrace();
        System.exit(1);
    }
}
