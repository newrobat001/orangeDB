package com.cdd.orangeDB.backend.dm.page;

public interface Page {
    /**
     * 锁定页面，确保当前线程可以安全地读取或写入页面的内容。
     * 当一个页面被锁定后，其他线程将无法访问该页面，直到它被解锁。
     */
    void lock();

    /**
     * 解锁页面，允许其他线程访问该页面。
     */
    void unlock();

    /**
     * 释放页面，表示不再需要该页面，并可以进行回收或重新利用。
     */
    void release();

    /**
     * 标记页面为脏页，表示页面的内容已经被修改并需要被写回到存储介质。
     */
    void setDirty(boolean dirty);

    /**
     * 检查页面是否为脏页，即页面的内容是否已被修改。
     * @return 如果页面是脏页，则返回 true；否则返回 false。
     */
    boolean isDirty();

    /**
     * 获取页面的页号（或页标识符）。
     * @return 页面的页号。
     */
    int getPageNumber();

    /**
     * 获取页面的数据，通常以字节数组的形式表示页面的内容。
     * @return 包含页面数据的字节数组。
     */
    byte[] getData();

}
