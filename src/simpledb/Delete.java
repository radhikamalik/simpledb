package simpledb;


/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

	private TransactionId tid;
	private DbIterator child;
	// private int tableid;
	private TupleDesc td;
	private boolean fetched;
	private boolean open;

	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 * 
	 * @param t
	 *            The transaction this delete runs in
	 * @param child
	 *            The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		this.tid = t;
		this.child = child;

		Type[] typeArr = new Type[1];
		typeArr[0] = Type.INT_TYPE;
		td = new TupleDesc(typeArr);
		open = false;
		fetched = false;
		

	}

	public TupleDesc getTupleDesc() {
		return this.td;
	}

	public void open() throws DbException, TransactionAbortedException {
		if (open)
			throw new IllegalStateException(
					"cannot open an already open iterator");

		open = true;
		child.open();
	}

	public void close() {
		open = false;
		child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		if (!open)
			throw new IllegalStateException("cannot rewind closed operator");
		fetched = false;
		child.rewind();
	}

	/**
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		BufferPool pool = Database.getBufferPool();
		if (!open)
			throw new IllegalStateException("cannot fetch next from closed operator");
		if (fetched)
			return null;
		int count = 0;
		// child.open();
		while (child.hasNext()) {
			Tuple t = child.next();

			pool.deleteTuple(tid, t);
			count++;

		}
		Tuple tup = new Tuple(this.td);
		tup.setField(0, new IntField(count));
		fetched = true;
		return tup;
	}
}
