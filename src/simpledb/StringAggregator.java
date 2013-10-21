package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private List<Tuple> groupedTuples;
	private TupleDesc td;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException
	 *             if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		if (what != Op.COUNT)
			throw new IllegalArgumentException();
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;

		groupedTuples = new ArrayList<Tuple>();
		Type[] newGroupType;
		if (gbfield == Aggregator.NO_GROUPING) {
			newGroupType = new Type[1];
			newGroupType[0] = Type.INT_TYPE;
		} else {

			newGroupType = new Type[2];
			newGroupType[0] = gbfieldtype;
			newGroupType[1] = Type.INT_TYPE;

		}
		td = new TupleDesc(newGroupType);
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		
		//IntField tupAField = (IntField) tup.getField(afield);
		//int aValue = tupAField.getValue();
		if (this.gbfield == Aggregator.NO_GROUPING) {
			if (groupedTuples.size() == 0) {

				Tuple newTuple = new Tuple(td);
				newTuple.setField(0, new IntField(1));
				groupedTuples.add(newTuple);
			} else {
				Tuple newTuple = groupedTuples.get(0);
				int tupToModifyFieldValue;
				IntField tupToModifyField = (IntField) newTuple.getField(1);
				tupToModifyFieldValue = tupToModifyField.getValue();
				newTuple.setField(0, new IntField(tupToModifyFieldValue+1));
			}

		} else {
			Tuple tupToModify=null;
			Field tupGBField = tup.getField(gbfield);
			
			for (int i = 0; i < groupedTuples.size(); i++) {
				Tuple t = groupedTuples.get(i);
				Field tGBField = t.getField(0);
				
				if (tGBField.equals(tupGBField)) {
					tupToModify = t;	
				}
			}
			if (tupToModify==null){
				
				Tuple newTuple = new Tuple(td);
				newTuple.setField(0, tupGBField);
				newTuple.setField(1, new IntField(1));
				groupedTuples.add(newTuple);
			}
			else{
				//System.out.println("HI");
				int tupToModifyFieldValue;
				IntField tupToModifyField = (IntField) tupToModify.getField(1);
				tupToModifyFieldValue = tupToModifyField.getValue();
				tupToModify.setField(1, new IntField(tupToModifyFieldValue+1));
			}
			
		}
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		return (DbIterator) new TupleIterator(td, groupedTuples);
	}

}
