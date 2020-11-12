package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor accepts a predicate to apply and a child operator to read tuples
	 * to filter from.
	 * 
	 * @param p     The predicate to filter tuples with
	 * @param child The child operator
	 */
	private Predicate p;
	private OpIterator child;

	public Filter(Predicate p, OpIterator child) {
		this.p = p;
		this.child = child;
		// some code goes here
	}

	public Predicate getPredicate() {
		// some code goes here
		return p;
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return child.getTupleDesc();
	}

	public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
		super.open();
		child.open();
		//fetchNext();
		// some code goes here
	}

	public void close() {
		super.close();
		child.close();
		// some code goes here
	}

	public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();// some code goes here
	}

	/**
	 * AbstractDbIterator.readNext implementation. Iterates over tuples from the
	 * child operator, applying the predicate to them and returning those that pass
	 * the predicate (i.e. for which the Predicate.filter() returns true.)
	 * 
	 * @return The next tuple that passes the filter, or null if there are no more
	 *         tuples
	 * @see Predicate#filter
	 */
	protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
		/*
		 * while(child.hasNext()) { if(p.filter(child.next())) return child.next(); }
		 * 
		 * 
		 * // some code goes here
		 * 
		 * return null;
		 */
		while (child.hasNext()) {
			Tuple tempt = child.next();
			if (p.filter(tempt)) {
				return tempt;
			}
		}
		return null;
	}

	@Override
	public OpIterator[] getChildren() {
		return new OpIterator[] { child };
		// some code goes here
		// return null;
	}

	@Override
	public void setChildren(OpIterator[] children) {
		child = children[0];
		// some code goes here
	}

}
