package simpledb;

import java.util.HashSet;
import java.util.Set;
/**
 * PageLock is the class that represents a lock structure.
 * Each PageLock has a type and a set of transaction id's of transactions holding the lock
 * 
 * @author RadhikaMalik
 *
 */
public class PageLock {

	public enum LockType{
		SHARED, EXCLUSIVE;
	}
	
	private LockType type; //indicates type of lock. will be null if no transaction holds lock
	private Set<TransactionId> tids; //transaction id's holding this lock. Will be size 1 if lock is exclusive
	
	/**
	 * Creates PageLock with a given type and empty transaction set.
	 * @param type
	 */
	public PageLock(LockType type){
		this.type=type;
		this.tids=new HashSet<TransactionId>();
	}
	/**
	 * Gives a transaction a particular type of lock. The lock type is updated and the 
	 * transactionid is added to the set of transactions holding the lock.
	 * @param type
	 * @param tid
	 */
	public void acquireLock(LockType type, TransactionId tid){
		synchronized(this) {
		this.type = type;
		this.tids.add(tid);
		}
		
	}
	/**
	 * Returns the type of the lock
	 * @return
	 */
	public LockType getType(){
		return this.type;
	}
	/**
	 * Return the set of transactions holding the lock
	 * @return
	 */
	public Set<TransactionId> getTransactionsHoldingLock(){
		return this.tids;
	}
	/**
	 * Uprade the lock from shared to exclusive.
	 */
	public void upgradeLock(){
		if (this.type == LockType.SHARED)
				this.type = LockType.EXCLUSIVE;
	}
	/**
	 * Releases the lock from a transaction.
	 * Removes the given transaction id from the set of id's holding the lock
	 * Changes the type of the lock to null if no transactions hold the lock anymore 
	 * @param tid
	 */
	public void releaseLock(TransactionId tid){
		synchronized(this) {
		tids.remove(tid);
		if (tids.size() == 0)
			this.type = null;
		}
	}
	/**
	 * Releases the lock from all transactions.
	 * Removes id's of all transactions from the set of transactions holding the lock and sets lock type to null 
	 */
	public void releaseAll(){
		synchronized(this) {
		tids.clear();
		this.type = null;
		}
	}
	
}
