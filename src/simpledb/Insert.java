package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

	private TransactionId tid;
	private DbIterator child;
	private int tableid;
	private TupleDesc td;
	private boolean fetched;
	private boolean open;

	/**
	 * Constructor.
	 * 
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableid
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	public Insert(TransactionId t, DbIterator child, int tableid)
			throws DbException {
		this.tid = t;
		this.child = child;
		this.tableid = tableid;
		this.fetched=false;
		Catalog catalog = Database.getCatalog();
		DbFile table = catalog.getDbFile(tableid);
		
		Type[] typeArr=new Type[1];
		typeArr[0]=Type.INT_TYPE;
		td=new TupleDesc(typeArr);
		open=false;
		//System.out.println(child.getTupleDesc());
		//System.out.println(table.getTupleDesc());
		if (!child.getTupleDesc().equals(table.getTupleDesc()))
			throw new DbException(
					"tuple descriptor of child differs from table in which insert has to be performed");

	}

	public TupleDesc getTupleDesc() {

		return this.td;
	}

	public void open() throws DbException, TransactionAbortedException {
		
		if (open)
			throw new IllegalStateException("cannot open an already open iterator");
		
		open=true;
		child.open();
	}

	public void close() {
		open=false;
		child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		if (!open)
			throw new IllegalStateException("cannot rewind close operator");
		fetched=false;
		child.rewind();
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 * 
	 * @return A 1-field tuple containing the number of inserted records, or
	 *         null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		BufferPool pool=Database.getBufferPool();
		if (!open)
			throw new IllegalStateException("cannot fetch next from closed operator");
		if (fetched)
			return null;
		int count=0;
		//child.open();
		while(child.hasNext()){
			Tuple t=child.next();
			try {
				pool.insertTuple(tid, tableid, t);
				count++;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Tuple tup=new Tuple(this.td);
		tup.setField(0, new IntField(count));
		fetched=true;
		return tup;
		
	}
}
