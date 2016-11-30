package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

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

    private class BPPage {
        Page page;
        int accessTime;

        BPPage(Page p, int at) {
            page = p;
            accessTime = at;
        }
    }

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int maxPages;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    int currSize;
    int currAccessTime;
    HashMap<PageId, BPPage> pages;

    /** TODO for Lab 4: create your private Lock Manager class. 
	Be sure to instantiate it in the constructor. */

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */



    public BufferPool(int numPages) {
        maxPages = numPages;
        currSize = 0;
        currAccessTime = 0;
        pages = new HashMap<PageId, BPPage>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }

    /**
     * Helper: this should be used for testing only!!!
     */
    public static void setPageSize(int pageSize) {
	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        int i;
        BPPage bppage = pages.get(pid);
        if(!(bppage == null)) {
            return bppage.page;
        }
        else {
            if(currSize >= maxPages)
                throw new DbException("Buffer Pool full");
            else {
                DbFile dbf=Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page p = dbf.readPage(pid);
                pages.put(pid,new BPPage(p,currAccessTime));
                currAccessTime++;
                currSize++;
                return p;
            }
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
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
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbf = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> list_of_pages = dbf.insertTuple(tid,t);
        int i, l = list_of_pages.size();
        for(i=0;i<l;i++) {
            Page page = list_of_pages.get(i);
            page.markDirty(true, tid);
            pages.put(page.getId(), new BPPage(page,currAccessTime));
            currAccessTime++;
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tID = t.getRecordId().getPageId().getTableId();
        DbFile dbf = Database.getCatalog().getDatabaseFile(tID);
        ArrayList<Page> list_of_pages = dbf.deleteTuple(tid, t);
        int i, l = list_of_pages.size();
        for(i=0;i<l;i++) {
            Page page = list_of_pages.get(i);
            pages.put(page.getId(),new BPPage(page,currAccessTime));
            currAccessTime++;
            page.markDirty(true,tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator it = pages.keySet().iterator();
        while(it.hasNext()) {
            flushPage(pages.get(it.next()).page.getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for labs 1--4
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbf=Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = pages.get(pid).page;
        TransactionId tid = page.isDirty();
        if(tid != null) {
            dbf.writePage(page);
            page.markDirty(false, tid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for labs 1--4
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int min = Integer.MAX_VALUE;
        Iterator it = pages.keySet().iterator();
        PageId min_pid = null;
        while(it.hasNext()) {
            BPPage bpp = pages.get(it.next());
            int time = bpp.accessTime;
            if(time < min) {
                time = min;
                min_pid = bpp.page.getId();
            }
        }
        Page page = pages.get(min_pid).page;
        TransactionId tid = page.isDirty();
        if(tid != null) {
            page.markDirty(false, tid);
            try {
                flushPage(min_pid);    
            }
            catch(Exception e) {
                System.out.println("Eviction Failed");
            }
            
        }
        pages.remove(min_pid);
    }

}
