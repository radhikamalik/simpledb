package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	
	DbFileIterator fileIterator;
	TransactionId tid;
	int tableid;
	String tableAlias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid=tid;
        this.tableid=tableid;
        
        //handle case of table alias being null
        if (tableAlias==null)
        	this.tableAlias="null";
        else
        	this.tableAlias=tableAlias;
        
        this.fileIterator=Database.getCatalog().getDbFile(tableid).iterator(tid);
    }

    public SeqScan(TransactionId tid, int tableid) {
	this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open()
        throws DbException, TransactionAbortedException {
        fileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc tableDesc=Database.getCatalog().getTupleDesc(tableid);
    	String[] fieldAr=tableDesc.fieldAr;
    	int length=fieldAr.length;
    	String[] fieldArTableAlias=new String[length];
    	
    	Type[] typeAr=tableDesc.typeAr;
    	for (int i=0;i< length;i++){
    		String field=fieldAr[i];
    		if (field==null) field="null"; //handle case of field name being null
    		fieldArTableAlias[i]=this.tableAlias+"."+field;
    	}
    	return new TupleDesc(typeAr,fieldArTableAlias);
    	
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return  fileIterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return fileIterator.next();
    }

    public void close() {
        fileIterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        fileIterator.rewind();
    }
}
