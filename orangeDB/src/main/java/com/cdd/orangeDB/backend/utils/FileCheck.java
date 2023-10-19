package com.cdd.orangeDB.backend.utils;

import com.cdd.orangeDB.common.ERROR;

import java.io.File;

public class FileCheck {
    public static void checkFile(File file){
        if (!file.exists()){
            Panic.panic(ERROR.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(ERROR.FileCannotRWException);
        }
    }

}
