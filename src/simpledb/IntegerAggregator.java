package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	

	private List<Tuple> groupedTuples;
	private TupleDesc td;

	//auxiliary lists to keep track of sum and count if we are doing averages
	private List<Integer> countForAvg;
	private List<Integer> sumForAvg;

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
	 *            the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
		if (this.what == Op.AVG) {
			countForAvg = new ArrayList<Integer>();
			sumForAvg = new ArrayList<Integer>();

		}
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {

		// get value of aggregate field and group by field in new tuple
		IntField tupAField = (IntField) tup.getField(afield);
		int aValue = tupAField.getValue();

		
		int IndexForAvg = 0;

		if (this.gbfield == Aggregator.NO_GROUPING) {

			if (groupedTuples.size() == 0) {

				Tuple newTuple = new Tuple(td);

				switch (what) {
				case MIN:
					newTuple.setField(0, new IntField(aValue));
					break;

				case MAX:
					newTuple.setField(0, new IntField(aValue));
					break;
				case SUM:
					//System.out.println(newTuple);
					newTuple.setField(0, new IntField(aValue));
					break;
				case COUNT:
					
					newTuple.setField(0, new IntField(1));
					//System.out.println(newTuple);
					break;
				case AVG:
					IndexForAvg = 0;
					countForAvg.add(1);
					sumForAvg.add(aValue);
					newTuple.setField(0, new IntField(aValue));
					break;
				}
				groupedTuples.add(newTuple);
			} else {

				Tuple newTuple = groupedTuples.get(0);
				IntField tupToModifyField = (IntField) newTuple.getField(0);

				int tupToModifyFieldValue = tupToModifyField.getValue();

				switch (what) {
				case MIN:

					if (aValue < tupToModifyFieldValue) {
						newTuple.setField(0, new IntField(aValue));
					}
					break;
				case MAX:

					if (aValue > tupToModifyFieldValue) {
						newTuple.setField(0, new IntField(aValue));
					}
					break;
				case SUM:
					//System.out.println(newTuple);
					newTuple.setField(0, new IntField(aValue
							+ tupToModifyFieldValue));
					break;
				case COUNT:

					newTuple.setField(0,
							new IntField(tupToModifyFieldValue + 1));
					
					break;
				case AVG:

					int sumForAvgVal = sumForAvg.get(IndexForAvg) + aValue;
					int countForAvgVal = countForAvg.get(IndexForAvg) + 1;
					sumForAvg.set(IndexForAvg, sumForAvgVal);
					countForAvg.set(IndexForAvg, countForAvgVal);
					newTuple.setField(0, new IntField(sumForAvgVal
							/ countForAvgVal));

					break;
				}
			}

		} else {
			Field tupGBField = tup.getField(gbfield);
			// iterate through list of grouped tuples and find tuple to merge
			// this
			// new tuple into
			Tuple tupToModify = null;

			for (int i = 0; i < groupedTuples.size(); i++) {
				Tuple t = groupedTuples.get(i);
				Field tGBField = t.getField(0);
				if (tGBField.equals(tupGBField)) {
					tupToModify = t;
					IndexForAvg = i;
				}
			}

			if (tupToModify == null) {

				tupToModify = new Tuple(td);
				tupToModify.setField(0, tupGBField);

				switch (what) {
				case MIN:
					tupToModify.setField(1, new IntField(aValue));
					break;

				case MAX:
					tupToModify.setField(1, new IntField(aValue));

					break;
				case SUM:
					
					tupToModify.setField(1, new IntField(aValue));
					break;
				case COUNT:

					tupToModify.setField(1, new IntField(1));
					break;
				case AVG:
					IndexForAvg = 0;
					countForAvg.add(1);
					sumForAvg.add(aValue);
					tupToModify.setField(1, new IntField(aValue));
					break;

				}
				
				groupedTuples.add(tupToModify);
			} else {
				IntField tupToModifyField = (IntField) tupToModify.getField(1);

				int tupToModifyFieldValue;
				tupToModifyField = (IntField) tupToModify.getField(1);
				tupToModifyFieldValue = tupToModifyField.getValue();

				switch (what) {
				case MIN:

					if (aValue < tupToModifyFieldValue) {
						tupToModify.setField(1, new IntField(aValue));
					}
					break;
				case MAX:
					
					if (aValue > tupToModifyFieldValue) {
						tupToModify.setField(1, new IntField(aValue));
					}
					break;
				case SUM:
					tupToModify.setField(1, new IntField(aValue
							+ tupToModifyFieldValue));
					break;
				case COUNT:

					tupToModify.setField(1, new IntField(
							tupToModifyFieldValue + 1));
					break;
				case AVG:

					int sumForAvgVal = sumForAvg.get(IndexForAvg) + aValue;
					int countForAvgVal = countForAvg.get(IndexForAvg) + 1;
					sumForAvg.set(IndexForAvg, sumForAvgVal);
					countForAvg.set(IndexForAvg, countForAvgVal);
					tupToModify.setField(1, new IntField(sumForAvgVal
							/ countForAvgVal));

					break;
				}
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

		return (DbIterator) new TupleIterator(td, groupedTuples);

	}

}
