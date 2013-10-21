package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
/**
 * Iterator to iterate over all tuples in a page
 * @author RadhikaMalik
 *
 */
public class HeapPageIterator implements Iterator<Tuple> {
	
	List<Tuple> tupleList;
	
	final Iterator<Tuple> iterator;

	public HeapPageIterator(HeapPage page) {
		
		//find all tuples stored in pages and create an iterator over that list
		tupleList= new ArrayList<Tuple>();

		for (int i = 0; i < page.numSlots; i++) {
			if (page.isSlotUsed(i)) {
				tupleList.add(page.tuples[i]);
			}
		}
		iterator=tupleList.iterator();
	}
	/** @return true if there are more tuples available on page */
	@Override
	public boolean hasNext() {
		return (iterator.hasNext());
	}
    /**
     * Gets the next tuple from the operator
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
	@Override
	public Tuple next() {
		
		if (iterator.hasNext())
			return iterator.next();
		else 
			throw new NoSuchElementException();

	}
	/**
	 * Removing is not supported in this iterator
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();

	}

}
