package simpledb;

import java.io.*;

import java.util.List;
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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final Page[] pages;
    private final boolean[] clock;
    private int clockPointer;
    private LockManager lockManager;
    private final int INTERVAL = 500;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new Page[numPages];
        clock = new boolean[numPages];
        lockManager = new LockManager();
        clockPointer = 0;
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        try {
            while (!lockManager.requireLock(tid, pid, perm))
            {
                if(lockManager.detectDeadLock(tid, pid))
                    throw new TransactionAbortedException();
                Thread.sleep(INTERVAL);
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        int idx = -1;

        for (int i=0; i<pages.length; i++) {
            if (null == pages[i]) {
                idx = i;
            } else if (pid.equals(pages[i].getId())) {
                if(!clock[i])
                    clock[i] = true;
                return pages[i];
            }
        }
        if (idx < 0) {
            evictPage();
            return getPage(tid, pid, perm);
        } else {
            if(!clock[idx])
                clock[idx] = true;
            return pages[idx] = Database.getCatalog().getDatabaseFile
                    (pid.getTableId()).readPage(pid);
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
        if(!lockManager.unlock(tid, pid))
            throw new IllegalArgumentException("No lock");
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.islock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        lockManager.releaseAllLock(tid);
        if (commit) {
            flushPages(tid);
        }
        else {
            revert(tid);
        }
    }

    public void revert(TransactionId tid)
    {
        for(int i=0;i<pages.length; ++i)
        {
            if(pages[i]!=null && tid.equals(pages[i].isDirty()))
            {
                pages[i] = Database.getCatalog().getDatabaseFile(pages[i].getId().getTableId()).readPage(pages[i].getId());
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for(Page pg: pages)
            if(pg!=null)
                pg.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for(Page pg: pages)
            if(pg!=null)
                pg.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(int i=0; i<pages.length; ++i)
        {
            if(pages[i]!=null && pages[i].isDirty() != null)
            {
                Database.getLogFile().logWrite(pages[i].isDirty(), pages[i].getBeforeImage(), pages[i]);
                Database.getLogFile().force();
                pages[i].markDirty(false, pages[i].isDirty());
                Database.getCatalog().getDatabaseFile(pages[i].getId().getTableId()).writePage(pages[i]);
                return;
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        for(int i=0; i<pages.length; ++i)
        {
            if(pages[i]!=null && pid.equals(pages[i].getId()))
            {
                pages[i] = null;
                break;
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {

        for (Page page : pages) {
            if (page!=null && pid.equals(page.getId())) {
                TransactionId dirtier = page.isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                    Database.getLogFile().force();
                }
                page.markDirty(false, page.isDirty());
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
                return;
            }
        }
        throw new IOException("Flush failed!");
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Page page : pages) {
            if (page!=null && tid.equals(page.isDirty())) {
                flushPage(page.getId());
                page.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    //clock Algorithm:遍历clock，将clock中的1置0，直到遇到第一个0，将对应的page删除
    //NO STEAL:脏页不能被写入磁盘，因此指针指到的如果是脏页，继续搜索，如果全部是脏页，抛出异常
    private synchronized void evictPage() throws DbException {
        /*int i=0;
        for(; i<pages.length; ++i)
        {
            if(pages[i].isDirty()==null)//&& !lockManager.islock(pages[i].getId()))
            {
                try
                {
                    flushPage(pages[i].getId());
                    pages[i] = null;
                    return;
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        if(i == pages.length)
            throw new DbException("All pages are dirty!");*/
        int k=0, count=0;
        for(Page page:pages)
        {
            if(page.isDirty()!=null)
                count++;
        }
        if(count == pages.length)
            throw new DbException("All pages are dirty!");
        for(int i=clockPointer;; ++i)
        {
            k = i % pages.length;
            if(clock[k])
            {
                clock[k] = false;
            }
            else
            {
                try
                {
                    if(pages[k].isDirty()==null) {
                        flushPage(pages[k].getId());
                        clockPointer = k;
                        pages[k] = null;
                        return;
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

}
