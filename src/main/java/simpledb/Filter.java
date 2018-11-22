package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate pred;
    private OpIterator[] children;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.pred = p;
        this.children = new OpIterator[1];
        this.children[0] = child;
    }

    public Predicate getPredicate() {
        return this.pred;
    }

    public TupleDesc getTupleDesc() {
        return children[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.children[0].open();
    }

    public void close() {
        super.close();
        this.children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.children[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple t = null;
        try {
            while (t == null) {
                Tuple next = this.children[0].next();
                if (this.pred.filter(next)) {
                    t = next;
                    break;
                }
            }
        } catch (NoSuchElementException ex) {
        }

        // Return null if no more tuples
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }

}
