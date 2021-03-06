package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t1=null;
    private Tuple t2=null;
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.p=p;
        this.child1=child1;
        this.child2=child2;
    	// some code goes here
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        
    	SeqScan s1=(SeqScan)child1;
    	return s1.getTableName();
    	// some code goes here
        //return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        
    	SeqScan s2=(SeqScan)child2;
    	return s2.getTableName();
    	// some code goes here
        //return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(),child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        System.out.println("111123445667");
        joinTuple=getAllTuple();
        //fetchNext();
    	// some code goes here
    }

    public void close() {
    	super.close();
    	child1.close();
        child2.close();
        joinTuple=null;
    	// some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        
    	child1.rewind();
    	child2.rewind();
    	System.out.println("111123445667vvvvvvvvvvvvvv");
    	joinTuple=getAllTuple();// some code goes here
    }

    private Iterator<Tuple> getAllTuple() throws DbException, TransactionAbortedException {
		List<Tuple> tupleList=new ArrayList<Tuple>();
		//tupleList.iterator();
		System.out.println(p.getOperator()+"p");
		/*
		 * switch (p.getOperator()){ case EQUALS: System.out.println("p.equal");
		 * //handleEqual(leftSize, rightSize); while(child1.hasNext()) {
		 * t1=child1.next(); child2.rewind();
		 * System.out.println("t1.getField(1)"+t1.getField(1)); while(child2.hasNext())
		 * { t2=child2.next(); //if() //if(p.getOperator()==) if(p.filter(t1, t2)) {
		 * System.out.println("pppppp"); tupleList.add(mergeTuple(t1,t2)); }
		 * 
		 * } } break; case GREATER_THAN: case GREATER_THAN_OR_EQ:
		 * System.out.println("p.Greater"); //handleGreaterThan(leftSize, rightSize);
		 * while(child1.hasNext()) { t1=child1.next(); child2.rewind();
		 * while(child2.hasNext()) { t2=child2.next(); //if(p.getOperator()==)
		 * if(p.filter(t1, t2)) { //System.out.println(p.getOperator()+"p");
		 * tupleList.add(mergeTuple(t1,t2)); }
		 * 
		 * } } break; case LESS_THAN: case LESS_THAN_OR_EQ:
		 * System.out.println("p.less"); //handleLessThan(leftSize, rightSize);
		 * while(child1.hasNext()) { t1=child1.next(); while(child2.hasNext()) {
		 * t2=child2.next(); //if(p.getOperator()==) if(p.filter(t1, t2)) {
		 * //System.out.println(p.getOperator()+"p"); tupleList.add(mergeTuple(t1,t2));
		 * }
		 * 
		 * } } break; }
		 */
		
    	while(child1.hasNext())
		{
			t1=child1.next();
			child2.rewind();
			while(child2.hasNext())
			{
				t2=child2.next();
				//if(p.getOperator()==)
				if(p.filter(t1, t2)) {
					//System.out.println(p.getOperator()+"p");
					tupleList.add(mergeTuple(t1,t2));
				}
				
			}
		}
    	//joinTuple=tupleList.iterator();
    	// TODO Auto-generated method stub
    	System.out.println("hang"+tupleList.size()+tupleList);
		return tupleList.iterator();
	}

	/**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Iterator<Tuple> joinTuple=null;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(joinTuple==null)return null;
        if(joinTuple.hasNext())
        {
        	return joinTuple.next();
        }
        return null;
		/*
		 * while(child1.hasNext()) { t1=child1.next(); while(child2.hasNext()) {
		 * t2=child2.next(); if(p.filter(t1, t2)) {
		 * //System.out.println(p.getOperator().toString());
		 * System.out.println("mergeTuple(t1,t2).toString()"+mergeTuple(t1,t2).toString(
		 * )); //return mergeTuple(t1,t2); joinTuple.add(mergeTuple(t1,t2));
		 * 
		 * } } } System.out.println("joinTuple.toString()"+joinTuple.toString());
		 * 
		 * // some code goes here return null;
		 */
    }

    private Tuple mergeTuple(Tuple tp1, Tuple tp2) {
        int tpSize1 = tp1.getTupleDesc().numFields();
        int tpSize2 = tp2.getTupleDesc().numFields();

        Tuple tempTp = new Tuple(getTupleDesc());
        int i = 0;
        for (; i < tpSize1; i++){
            tempTp.setField(i, tp1.getField(i));
        }

        for (; i < tpSize2 + tpSize1 ; i++){
            tempTp.setField(i, tp2.getField(i-tpSize1));
        }

        return tempTp;
        }
    
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	return new OpIterator[] { child1,child2 };
    	//return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	child1 = children[0];
    	child2 = children[1];
    	// some code goes here
    }

}
