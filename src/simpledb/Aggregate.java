package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	private DbIterator tupleIterator;
	private DbIterator child;
	private Aggregator.Op aop;
	private int gfield;
	private int afield;

	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an IntAggregator or StringAggregator to help you with your
	 * implementation of readNext().
	 * 
	 * 
	 * @param child
	 *            The DbIterator that is feeding us tuples.
	 * @param afield
	 *            The column over which we are computing an aggregate.
	 * @param gfield
	 *            The column over which we are grouping the result, or -1 if
	 *            there is no grouping
	 * @param aop
	 *            The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here

		this.child = child;

		
		TupleDesc td = child.getTupleDesc();
		Type aFieldType = td.getFieldType(afield);
		Type gbFieldType=null;
		Aggregator agg;
		this.aop=aop;
		this.gfield=gfield;

		if (gfield == -1) {
			gfield = Aggregator.NO_GROUPING;
			
		} else {
			gbFieldType=td.getFieldType(gfield);
			
		}
		
		//make a new integer or string operator based on type of aggregate field
		if (aFieldType == Type.INT_TYPE) {
			agg = new IntegerAggregator(gfield, gbFieldType, afield, aop);
		} else {
			agg = new StringAggregator(gfield, gbFieldType, afield, aop);
		}
		try {
			this.child.open();
			while (child.hasNext()) {

				Tuple next = child.next();
				agg.mergeTupleIntoGroup(next);

			}
			tupleIterator = agg.iterator();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			this.child.close();
		}
	}

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		switch (aop) {
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	public void open() throws NoSuchElementException, DbException,
			TransactionAbortedException {
		tupleIterator.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {

		if (tupleIterator.hasNext())
			return tupleIterator.next();
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		tupleIterator.rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		TupleDesc td=tupleIterator.getTupleDesc();
		int length=td.fieldAr.length;
		String[] fieldAr=new String[length];
		
		Type[] typeAr=td.typeAr;

			if (this.gfield==-1){
				fieldAr[0]=this.aop+"("+child.getTupleDesc().getFieldName(afield)+")";
			}
			else{
				fieldAr[0]=child.getTupleDesc().getFieldName(gfield);
				fieldAr[1]=this.aop+"("+child.getTupleDesc().getFieldName(afield)+")";
			}
			
		
		return new TupleDesc(typeAr,fieldAr);
	}

	public void close() {
		tupleIterator.close();
	}
}
