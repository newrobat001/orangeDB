package com.cdd.orangeDB.common;

public class ERROR extends Exception {
    // common
    public static final Exception CacheFullException = new RuntimeException("缓存已满！");
    public static final Exception FileExistsException = new RuntimeException("文件已存在！");
    public static final Exception FileNotExistsException = new RuntimeException("文件不存在！");
    public static final Exception FileCannotRWException = new RuntimeException("文件无法读取或写入！");
    // dm
    public static final Exception BadLogFileException = new RuntimeException("错误的日志文件！");
    public static final Exception MemTooSmallException = new RuntimeException("内存不足！");
    public static final Exception DataTooLargeException = new RuntimeException("数据过大！");
    public static final Exception DatabaseBusyException = new RuntimeException("数据库繁忙！");
    // tm
    public static final Exception BadOIDFileException = new RuntimeException("错误的OID文件！");
    // vm
    public static final Exception DeadlockException = new RuntimeException("死锁！");
    public static final Exception ConcurrentUpdateException = new RuntimeException("并发更新问题！");
    public static final Exception NullEntryException = new RuntimeException("空条目！");
    // tbm
    public static final Exception InvalidFieldException = new RuntimeException("无效的字段类型！");
    public static final Exception FieldNotFoundException = new RuntimeException("字段未找到！");
    public static final Exception FieldNotIndexedException = new RuntimeException("字段未建立索引！");
    public static final Exception InvalidLogOpException = new RuntimeException("无效的逻辑操作！");
    public static final Exception InvalidValuesException = new RuntimeException("无效的值！");
    public static final Exception DuplicatedTableException = new RuntimeException("重复的表！");
    public static final Exception TableNotFoundException = new RuntimeException("表未找到！");
    // parser
    public static final Exception InvalidCommandException = new RuntimeException("无效的命令！");
    public static final Exception TableNoIndexException = new RuntimeException("表没有建立索引！");
    // transport
    public static final Exception InvalidPkgDataException = new RuntimeException("无效的数据包！");
    // server
    public static final Exception NestedTransactionException = new RuntimeException("不支持嵌套事务！");
    public static final Exception NoTransactionException = new RuntimeException("不在事务中！");
    // launcher
    public static final Exception InvalidMemException = new RuntimeException("内存无效！");

}
