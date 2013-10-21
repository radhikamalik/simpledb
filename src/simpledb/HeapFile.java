package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	File f;
	TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.f = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {

		return this.f;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {

		return f.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {

		int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
		byte[] data = new byte[BufferPool.PAGE_SIZE];
		try {
			RandomAccessFile access = new RandomAccessFile(this.f, "r");
			access.seek(offset);

			int i = 0;
			while (i < BufferPool.PAGE_SIZE) {
				data[i] = access.readByte();
				i++;
			}
			access.close();
			return new HeapPage((HeapPageId) pid, data);

		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException();
		} catch (IOException e) {
			throw new IllegalArgumentException();
		}
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		int pno = page.getId().pageNumber();
		int offset = pno * BufferPool.PAGE_SIZE;
		byte[] data = page.getPageData();

		RandomAccessFile access = new RandomAccessFile(this.f, "rw");
		
		//write the page data onto the given file
		access.seek(offset);
		access.write(data);
		access.close();

	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) (this.f.length() / BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {

		int num = this.numPages();

		BufferPool pool = Database.getBufferPool();
		
		for (int i = 0; i < num; i++) {
			HeapPageId currentPageId=new HeapPageId(
					this.getId(), i);
			//scan pages for empty slots using Read_Only permissions
			HeapPage currentPage = (HeapPage) pool.getPage(tid, currentPageId, Permissions.READ_ONLY);
			if (currentPage.getNumEmptySlots() > 0) {
				
				//if page has empty slots get Read_Write permission on it
				currentPage = (HeapPage) pool.getPage(tid, currentPageId, Permissions.READ_WRITE);
				currentPage.insertTuple(t);
				ArrayList<Page> pages = new ArrayList<Page>();
				pages.add(currentPage);
				return pages;
			}
			else{
				
				//if page does not have empty slots, release the previously acquired Read_Only lock
				pool.releasePage(tid, currentPageId);
				
			}
		}
		// no page is empty in heap file, so create a new heap page
		byte[] byteArr = HeapPage.createEmptyPageData();

		HeapPageId pid = new HeapPageId(this.getId(), num);

		HeapPage newPage = new HeapPage(pid, byteArr);

		// insert tuple in new page
		newPage.insertTuple(t);
		// write the new page to the file
		writePage(newPage);
		
		// return this new page as a single page modified by this file.
		ArrayList<Page> pages = new ArrayList<Page>();
		pages.add(newPage);
		return pages;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		RecordId rid = t.getRecordId();
		PageId pid = rid.getPageId();

		BufferPool pool = Database.getBufferPool();
		HeapPage page = (HeapPage) pool.getPage(tid, pid, Permissions.READ_WRITE);
		if (page == null)
			throw new DbException("tuple is not a member of this file");
		page.deleteTuple(t);
		return page;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid, this);
	}

}
