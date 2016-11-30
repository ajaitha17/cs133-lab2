package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator dbit;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid=tid;
        this.tableid=tableid;
        this.tableAlias=tableAlias;
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        dbit = hf.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        String tName=Database.getCatalog().getTableName(tableid);
        return tName;
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return this.tableAlias;
       
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid=tableid;
        this.tableAlias=tableAlias;

    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        dbit.open();

    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td= Database.getCatalog().getTupleDesc(tableid);
        String[] fieldAr=new String[td.numFields()];
        Type[] typeAr=new Type[td.numFields()];
        int i=0;
        while(i<=(td.numFields()-1)){
            fieldAr[i]=td.getFieldName(i);
            typeAr[i]=td.getFieldType(i);
            i++;
        }
        int j=0;
        while(j<=fieldAr.length-1){
            StringBuilder b= new StringBuilder();
            fieldAr[j]=b.append(tableAlias).append(fieldAr[j]).toString();
            //System.out.println(fieldAr[j]);
            j++;
        }
        TupleDesc getTupD=new TupleDesc(typeAr, fieldAr);
        return getTupD;
        
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return dbit.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (dbit.hasNext()==true){
            return dbit.next();
        }
        else{
            throw new NoSuchElementException();
        }
    }

    public void close() {
        // some code goes here
        dbit.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbit.rewind();
    }
}
