package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    public static final int DEFAULT_PAGE_SIZE = 4096;
    /**
     * Bytes per page, including header.
     */

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    //建立id到page的一一映射
    private HashMap<PageId, Page> pageId;
    //页的最大数量
    private int MAX_Page;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        MAX_Page = numPages;
        pageId = new HashMap<>(MAX_Page);
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        // some code goes here
        boolean state = lockManager.acquireLock(tid, pid, perm);
        // 如果申请加锁失败
        long start = System.currentTimeMillis();
        while (!state) {
            long end = System.currentTimeMillis();
            // 如果申请时间超过2秒
            if (end - start > 2000) {
                // 删除tid事务的申请，中止tid事务
                lockManager.removeWaiter(tid, pid);
                throw new TransactionAbortedException();
            }
            try {
                // 每隔10毫秒申请一次，直到申请成功
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            state = lockManager.acquireLock(tid, pid, perm);
        }
        if (pageId.containsKey(pid)) {// 判断要返回的page是否已存在
            return pageId.get(pid);// 如果存在直接返回page
        } else {// 如果不存在，把需要返回的page加进去，再返回对应page
            DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());// 找到tableid对应的table
            Page newPage = table.readPage(pid);
            if (pageId.size() == MAX_Page) {// 判断缓冲池里是否还有空间，如果没有空间，就清除最后一个page
                this.evictPage();
            }
            pageId.put(pid, newPage);// 把新的page放入
            pageId.get(pid).setBeforeImage();
            if (perm == Permissions.READ_WRITE) {
                newPage.markDirty(true, tid);
            }
            return newPage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            for (PageId pid : pageId.keySet()) {
                // 遍历缓冲池中的page，如果是对应脏页
                if (pageId.get(pid).isDirty() != null &&
                        pageId.get(pid).isDirty().equals(tid)) {
                    // 如果事务是提交，刷新page
                    if (commit) {
                        flushPage(pid);
                    } else {
                        // 如果是中止事务，将页面恢复到其磁盘状态来还原事务所做的任何更改
                        pageId.put(pid, pageId.get(pid).getBeforeImage());
                    }
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
        // 释放该事务持有的所有锁
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile heapFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = heapFile.insertTuple(tid, t);
        for (Page page : pages) {
            if (!pageId.containsKey(page.getId()) && pageId.size() == MAX_Page) {
                evictPage();
            }
            page.markDirty(true, tid);
            pageId.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile heapFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = heapFile.deleteTuple(tid, t);
        for (Page page : pages) {
            if (!pageId.containsKey(page.getId()) && pageId.size() == MAX_Page) {
                evictPage();
            }
            page.markDirty(true, tid);
            pageId.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pageId.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageId.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageId.get(pid);
        if (page != null && page.isDirty() != null) {
            DbFile heapFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            heapFile.writePage(page);
            page.markDirty(false, null);
            page.setBeforeImage();
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pageId.keySet()) {
            Page p = pageId.get(pid);
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                flushPage(pid);
                if (p.isDirty() == null) {
                    p.setBeforeImage();
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        ArrayList<PageId> cleanPages = new ArrayList<PageId>();
        for (PageId pid : pageId.keySet()) {
            if (pageId.get(pid).isDirty() == null) {
                cleanPages.add(pid);
            }
        }
        if (cleanPages.size() == 0) {
            throw new DbException("");
        }
        PageId vic = cleanPages.get(cleanPages.size() - 1);
        try {
            flushPage(vic);
        } catch (Exception e) {
            e.printStackTrace();
        }
        discardPage(vic);
    }


}
