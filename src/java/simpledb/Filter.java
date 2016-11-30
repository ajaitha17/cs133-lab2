package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    Predicate p;
    DbIterator child;
    TupleDesc td;
    Iterator<Tuple> it;
    ArrayList<Tuple> tuples;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
        this.td = child.getTupleDesc();
        this.tuples = new ArrayList<Tuple>();
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        Tuple next;
        while(child.hasNext()) {
            next = child.next();
            if(this.p.filter(next))
                tuples.add(next);
        }
        it = tuples.iterator();
        super.open();
    }

    public void close() {
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it = tuples.iterator();
    }

    /**
     * The Filter operator iterates through the tuples from its child, 
     * applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * This method returns the next tuple.
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if(it != null && it.hasNext())
            return it.next();
        return null;
    }
    
    /**
     * See Operator.java for additional notes 
     */
    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }
    
    /**
     * See Operator.java for additional notes 
     */
    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
