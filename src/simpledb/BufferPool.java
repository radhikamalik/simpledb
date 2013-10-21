package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 */
public class BufferPool {
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	int numPages;
	List<Page> pages;
	int numPagesUsed;

	//Locking Structure that keeps track of all locks and transactions
	private LockTracker transactionPool; 
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 * 
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.numPages = numPages;
		this.pages = new ArrayList<Page>(numPages);
		numPagesUsed = 0;
		this.transactionPool = new LockTracker();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public synchronized Page getPage(TransactionId tid, PageId pid,
			Permissions perm) throws TransactionAbortedException, DbException {
		// find page in buffer pool
		for (Page p : this.pages) {
			if (p.getId().equals(pid)) {
				// page already in buffer pool so put it at beginning of list as
				// it is most recently accessed
				/*
				 * If you need an exclusive Lock, check if you can get an
				 * exclusive Lock for the transaction. If you cannot get it, then 
				 * wait for some time before trying to obtain lock again.
				 */
				if (perm == Permissions.READ_WRITE) {
					int i = 0;
					boolean aquired = false;
					while (i < 200) {
						if (transactionPool.canTransactionGetXLock(tid, pid)) {
							transactionPool.giveXLockToTransaction(tid, pid);
							aquired = true;
							break;
						} else
							try {
								wait(1);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						i++;
					}
					if (!aquired)
						throw new TransactionAbortedException();
				}

				else if (perm == Permissions.READ_ONLY) {
					int i = 0;
					boolean aquired = false;
					while (i < 200) {
						if (transactionPool.canTransactionGetSLock(tid, pid)) {
							transactionPool.giveSLockToTransaction(tid, pid);
							aquired = true;
							break;
						} else
							try {
								wait(1);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						i++;
					}
					if (!aquired)
						throw new TransactionAbortedException();
				}

				this.pages.remove(p);
				this.pages.add(0, p);
				return p;
			}

		}

		// buffer pool full capacity, so cannot retreive more pages from disk,
		// instead evict a page!
		if (numPagesUsed >= numPages)
			evictPage();

		// get page from disk
		int tableID = pid.getTableId();
		Catalog catalog = Database.getCatalog();
		DbFile table = catalog.getDbFile(tableID);

		Page p = table.readPage(pid);
		pages.add(0,p);

		transactionPool.createLockForPage(pid);
		
		if (perm == Permissions.READ_WRITE) {
			int i = 0;
			boolean aquired = false;
			while (i < 200) {
				if (transactionPool.canTransactionGetXLock(tid, pid)) {
					transactionPool.giveXLockToTransaction(tid, pid);
					aquired = true;
					break;
				} else
					try {
						wait(1);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				i++;
			}
			if (!aquired)
				throw new TransactionAbortedException();
		}

		else if (perm == Permissions.READ_ONLY) {
			int i = 0;
			boolean aquired = false;
			while (i < 200) {
				if (transactionPool.canTransactionGetSLock(tid, pid)) {
					transactionPool.giveSLockToTransaction(tid, pid);
					aquired = true;
					break;
				} else
					try {
						wait(1);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				i++;
			}
			if (!aquired)
				throw new TransactionAbortedException();
		}
		numPagesUsed++; // increment pages in buffer pool
		return p;

	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public synchronized  void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		transactionPool.releaseLock(tid, pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public  synchronized  void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);

	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public  synchronized boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return transactionPool.hasLock(tid, p);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 * 
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public synchronized  void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2

		if (commit) { // transaction has to commit, release lock and flush
			// pages
			// dirtied by this transaction

			this.flushPages(tid);
			transactionPool.releaseAllLocksForTransaction(tid);
		} else {

			// transaction has to abort to release locks and remove pages
			// dirtied by the transaction from the buffer pool
			List<Page> pagesToRemove = new ArrayList<Page>();
			for (Page p : this.pages) {
				TransactionId t = p.isDirty();

				if (t != null) {
					if (t.equals(tid)) {
						pagesToRemove.add(p);
					}
				}

			}

			for (Page p : pagesToRemove) {
				this.pages.remove(p);
				this.numPagesUsed--;
			}

			transactionPool.releaseAllLocksForTransaction(tid);
		}

	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to(Lock acquisition
	 * is not needed for lab2). May block if the lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have
	 * been dirtied so that future requests see up-to-date pages.
	 * 
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public  synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {

		Catalog catalog = Database.getCatalog();
		DbFile table = catalog.getDbFile(tableId);

		// add tuple to heap file and get the pages that have been affected
		ArrayList<Page> pagesModified = table.insertTuple(tid, t);

		// for each affected page, mark it as dirty, dirtied by this transaction
		for (Page p : pagesModified) {
			p.markDirty(true, tid);
			// also read this new page into the bufferpool
			this.getPage(tid, p.getId(), Permissions.READ_WRITE);
		}

	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from. May block if the lock cannot
	 * be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit. Does not need to update cached versions of any pages
	 * that have been dirtied, as it is not possible that a new page was created
	 * during the deletion (note difference from addTuple).
	 * 
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public synchronized void deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		
		PageId pid = t.getRecordId().getPageId();
		int tableId=pid.getTableId();
		Catalog catalog = Database.getCatalog();
		DbFile table = catalog.getDbFile(tableId);
		
		//delete tuple from heap file
		Page p=table.deleteTuple(tid, t);
		
		//mark modified page as dirty
		p.markDirty(true, tid);
		// also read this new modified page into the bufferpool
		this.getPage(tid, p.getId(), Permissions.READ_WRITE);
		

	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {

		for (Page p : this.pages) {
			this.flushPage(p.getId());
		}

	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// only necessary for lab5
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {

		int tableid = pid.getTableId();

		Catalog catalog = Database.getCatalog();
		DbFile table = catalog.getDbFile(tableid);
		for (Page p : this.pages) {
			if (p.getId().equals(pid)) {
				table.writePage(p);
				this.transactionPool.releaseAllLocksOnPage(pid);
				p.markDirty(false, null);
				break;
			}
		}

	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2

		// flush all pages dirtied by this transaction
		for (Page p : this.pages) {

			TransactionId t = p.isDirty();
			if (t != null) {
				if (t.equals(tid))
					this.flushPage(p.getId());
				;
			}

		}

	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {

		for (int i = numPages - 1; i >= 0; i--) {
			Page page = this.pages.get(i);
			// PageId pageid = page.getId();

			if (page.isDirty() == null) {
				// if not dirty, evict!
				this.pages.remove(i);
				numPagesUsed--;
				return;
			}

		}
		// all pages in buffer pool dirty so cannot evict any, throw exception!

		throw new DbException("All pages dirty, cannot evict!");

	}

}
