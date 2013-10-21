package simpledb;

import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	Type[] typeAr;
	String[] fieldAr;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may
	 *            be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {

		if (typeAr.length != fieldAr.length)
			throw new RuntimeException(
					"type array and field array must be of same length");

		if (typeAr.length <= 0)
			throw new RuntimeException(
					"type array must have at least 1 element");

		this.typeAr = typeAr;
		this.fieldAr = fieldAr;
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		this.typeAr = typeAr;
		this.fieldAr = new String[typeAr.length];
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {

		return typeAr.length;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {

		try {
			String field = fieldAr[i];
			return field;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		try {
			Type type = typeAr[i];
			return type;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException();
		}
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		// iterate over all fields and return the first field whose name matches
		for (int i = 0; i < fieldAr.length; i++) {
			if (fieldAr[i] != null) {
				if (fieldAr[i].equals(name)) {
					return i;
				}
			}

		}
		throw new NoSuchElementException("No element found!");

	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		int size = 0;
		for (Type type : typeAr) {
			size += type.getLen();
		}
		return size;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		Type[] td1type = td1.typeAr;
		Type[] td2type = td2.typeAr;
		String[] td1field = td1.fieldAr;
		String[] td2field = td2.fieldAr;

		Type[] resultType = Arrays.copyOf(td1type, td1type.length
				+ td2type.length);
		System
				.arraycopy(td2type, 0, resultType, td1type.length,
						td2type.length);

		String[] resultField = Arrays.copyOf(td1field, td1field.length
				+ td2field.length);
		System.arraycopy(td2field, 0, resultField, td1field.length,
				td2field.length);

		return new TupleDesc(resultType, resultField);

	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof TupleDesc))
			return false;

		TupleDesc other = (TupleDesc) o;

		return ((Arrays.equals(this.typeAr, other.typeAr)));

	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results

		String returnVal = "";
		for (int i = 0; i < typeAr.length; i++) {
			returnVal += typeAr[i].toString();
		}
		return returnVal.hashCode();
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		String returnVal = "";
		for (int i = 0; i < fieldAr.length; i++) {
			returnVal += typeAr[i].toString() + "(";
			if (fieldAr[i] != null) {
				returnVal += fieldAr[i].toString();
			} else {
				returnVal += ".null";
			}
			returnVal += ")";
			if (i < fieldAr.length - 1)
				returnVal += ",";
		}
		return returnVal;
	}
}
