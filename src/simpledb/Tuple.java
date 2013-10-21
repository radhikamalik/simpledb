package simpledb;

import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple {

	TupleDesc td;
	RecordId rid;
	Field[] fieldAr;

	/**
	 * Create a new tuple with the specified schema (type).
	 * 
	 * @param td
	 *            the schema of this tuple. It must be a valid TupleDesc
	 *            instance with at least one field.
	 */
	public Tuple(TupleDesc td) {

		int length = td.fieldAr.length;
		if (length <= 0)
			throw new RuntimeException(
					"tuple descruptor must have at least 1 field");

		this.td = td;
		this.fieldAr = new Field[length];
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {

		return this.td;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 *         be null.
	 */
	public RecordId getRecordId() {

		return this.rid;
	}

	/**
	 * Set the RecordId information for this tuple.
	 * 
	 * @param rid
	 *            the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		this.rid = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 * 
	 * @param i
	 *            index of the field to change. It must be a valid index.
	 * @param f
	 *            new value for the field.
	 */
	public void setField(int i, Field f) {
		try {
			this.fieldAr[i] = f;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * @return the value of the ith field, or null if it has not been set.
	 * 
	 * @param i
	 *            field index to return. Must be a valid index.
	 */
	public Field getField(int i) {
		try {
			return this.fieldAr[i];
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 * 
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
	 * 
	 * where \t is any whitespace, except newline, and \n is a newline
	 */
	public String toString() {

		String returnVal = "";
		for (int i = 0; i < fieldAr.length; i++) {
			returnVal += fieldAr[i].toString();
			if (i < fieldAr.length - 1)
				returnVal += '\t';
		}
		return returnVal + '\n';
	}
}
