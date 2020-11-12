package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId t;
    private OpIterator child;
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.t=t;
    	this.child=child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    }
    
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    private Tuple tuple=null;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
    	if (tuple != null)
            return null;
    	int num=0;
    	while(child.hasNext())
    	{
    		try {
				Database.getBufferPool().deleteTuple(t, child.next());
				num++;
			} catch (NoSuchElementException | DbException | IOException | TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		
    	}
    	tuple=new Tuple(getTupleDesc());
    	IntField field=new IntField(num);
    	tuple.setField(0, field);
    	return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        //return null;
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.child=children[0];
    	// some code goes here
    }

}
