package simpledb;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import simpledb.PageLock.LockType;

/**
 * This class is the main interface for maintaining lock structures. All locking mechanisms are managed
 * through this class.
 * @author RadhikaMalik
 *
 */

public class LockTracker {

	private Hashtable<TransactionId, Set<PageLock>> transactionToLocks;
	private Hashtable<PageId, PageLock> pageToLock;

	public LockTracker() {
		this.transactionToLocks = new Hashtable<TransactionId, Set<PageLock>>();
		this.pageToLock = new Hashtable<PageId, PageLock>();
	}

	/**
	 * Checks if the transaction holds a lock on a given page
	 * 
	 * @param tid
	 * @param p
	 * @return
	 */
	public boolean hasLock(TransactionId tid, PageId pid) {
		synchronized (this) {
			PageLock l = pageToLock.get(pid);
			Set<TransactionId> tid_list = l.getTransactionsHoldingLock();
			return tid_list.contains(tid);
		}
	}
	/**
	 * Release a lock that a transaction holds on a given page
	 * @param tid
	 * @param pid
	 */
	public void releaseLock(TransactionId tid, PageId pid) {
		synchronized (this) {
			PageLock l = pageToLock.get(pid);
			
			// make sure lock keeps track of released transaction
			l.releaseLock(tid);

			// make sure transaction to lock mapping is updated

			Set<PageLock> locksForTransaction = transactionToLocks.get(tid);
			locksForTransaction.remove(l);
		}

	}
	/**
	 * Create a lock for a given page, if it doesn't exist already
	 * @param pid
	 */
	public void createLockForPage(PageId pid){
		 synchronized (this){
		PageLock l = pageToLock.get(pid);
		
		//entry already exists for pid to lock
		if (l!=null)
			return;
		
		//entry does not exist so create a new lock for this pid
		PageLock lock=new PageLock(null);
		this.pageToLock.put(pid, lock);
		 }
	}
	public void releaseAllLocksOnPage(PageId pid) {
		synchronized (this) {
			PageLock l = pageToLock.get(pid);
			l.releaseAll();
		}
	}
	/**
	 * Release all locks held by a given transaction
	 * @param tid
	 */
	public void releaseAllLocksForTransaction(TransactionId tid) {
		synchronized (this) {
			
			Set<PageLock> locksForTransaction = transactionToLocks.get(tid);

			Set<PageLock> locksToRemove = new HashSet<PageLock>();
			if (locksForTransaction != null) {
				for (PageLock l : locksForTransaction) {

					locksToRemove.add(l);

				}

				for (PageLock l : locksToRemove) {
					l.releaseLock(tid);
					locksForTransaction.remove(l);
				}
			}
		}
	}

	/**
	 * Gives an exclusive lock for a page to a transaction
	 * 
	 * @param tid
	 * @param pid
	 */
	public void giveXLockToTransaction(TransactionId tid, PageId pid) {
		synchronized (this) {
			

			Set<PageLock> locksForTransaction = transactionToLocks.get(tid);
			PageLock lock = pageToLock.get(pid);

			// If lock type is shared, then lock simply needs to be upgrades
			// It needs to be checked outside if this is even possible or not -
			// canGetXLock would have already checked it for us...
			if (lock.getType() == LockType.SHARED) {
				lock.upgradeLock();
				return;
			}
			
			if (locksForTransaction == null) {

				locksForTransaction = new HashSet<PageLock>();
				locksForTransaction.add(lock);
				transactionToLocks.put(tid, locksForTransaction);

			} else { // add the page to the list of pages locked by this
						// transaction
				locksForTransaction.add(lock);
				transactionToLocks.put(tid, locksForTransaction);
			}


			lock.acquireLock(LockType.EXCLUSIVE, tid);
			
		}
	}

	/**
	 * Gives a shared lock for a page to a transaction
	 * 
	 * @param tid
	 * @param pid
	 */
	public void giveSLockToTransaction(TransactionId tid, PageId pid) {
		synchronized (this) {
			
			if (tid == null) {
				return;
			}
			Set<PageLock> locksForTransaction = transactionToLocks.get(tid);
			PageLock lock = pageToLock.get(pid);

			//if the transaction already has an exclusive lock on the page the lock should not be updated to a shared lock 
			if (lock.getType() == LockType.EXCLUSIVE)
				return;
			
			if (locksForTransaction == null) {

				locksForTransaction = new HashSet<PageLock>();
				locksForTransaction.add(lock);
				transactionToLocks.put(tid, locksForTransaction);

			} else { // add the page to the list of pages locked by this
						// transaction
				locksForTransaction.add(lock);
				transactionToLocks.put(tid, locksForTransaction);
			}
			
			//add the transaction id to the set of transactions the lock holds
			lock.acquireLock(LockType.SHARED, tid);
		}
	}

	/**
	 * Checks whether a transaction can get an exclusive lock on a page
	 * 
	 * @param tid
	 * @return
	 */
	public boolean canTransactionGetXLock(TransactionId tid, PageId pid) {
		synchronized (this) {
			
			PageLock lock = pageToLock.get(pid);
			Set<TransactionId> tids = lock.getTransactionsHoldingLock();

			// no transactions hold this lock so this txn can get the lock
			if (tids.size() == 0)
				return true;

			if (lock.getType() == null)
				return true;

			// if lock type is s, then txn can get x lock if it is the only txn
			// holding s lock
			if (lock.getType() == LockType.SHARED) {
				return ((tids.size() == 1) && (tids.contains(tid)));
			}
			if (lock.getType() == LockType.EXCLUSIVE) {
				return tids.contains(tid);
			}
			return false;
		}
	}
	/**
	 * Check whether a transaction can get a shared lock on a page
	 * @param tid
	 * @param pid
	 * @return
	 */
	public boolean canTransactionGetSLock(TransactionId tid, PageId pid) {
		synchronized (this) {
			PageLock lock = pageToLock.get(pid);
			Set<TransactionId> tids = lock.getTransactionsHoldingLock();

			// no transactions hold this lock so this txn can get the lock
			if (tids.size() == 0)
				return true;

			if (lock.getType() == null)
				return true;

			// if lock type is s, then txn can get s lock
			if (lock.getType() == LockType.SHARED) {
				return true;
			}
			//if lock type is x then txn can get s lock only if it is the one holding x lock
			if (lock.getType() == LockType.EXCLUSIVE) {
				return ((tids.size() == 1) && (tids.contains(tid)));
			}
			return false;
		}
	}

}
