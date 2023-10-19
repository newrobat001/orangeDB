package com.cdd.orangeDB.backend.dm.pageIndex;

public class pageInfo {
    public int pgno;
    public int freeSpace;

    public pageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
