package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private HashMap<Field,Integer> allTuple;
	private TupleDesc td;
	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		//System.out.println("fffffffffffffff"+gbfieldtype);
		this.afield = afield;
		this.what = what;
		allTuple=new HashMap<Field,Integer>();
		// some code goes here
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		
		
		if (gbfield == Aggregator.NO_GROUPING) 
		{
			Type[] type=new Type[1];
			String[] fieldAr=new String[1];
			type[0]=tup.getTupleDesc().getFieldType(afield);
			fieldAr[0]=tup.getTupleDesc().getFieldName(afield);
			td=new TupleDesc(type, fieldAr);
		}
		else
		{
			Type[] type=new Type[2];
			String[] fieldAr=new String[2];
			type[0]=gbfieldtype;fieldAr[0]=tup.getTupleDesc().getFieldName(gbfield);
			type[1]=gbfieldtype;fieldAr[1]=tup.getTupleDesc().getFieldName(afield);
			td=new TupleDesc(type, fieldAr);
		}
		
		
		// some code goes here
		//td=tup.getTupleDesc();
		
		//td=createFlowTupleDesc(tup);
		System.out.println("fffffffffffffff"+td.getFieldType(1));
    	Field index;
    	if(gbfield==Aggregator.NO_GROUPING)
    	{
    		index=null;
    	}
    	else index=tup.getField(gbfield);
    	int count=0;
        //String num=((StringField)tup.getField(afield)).getValue();
        if(allTuple.isEmpty()) {
        	System.out.println("ssss"+what);
        	allTuple.put(index,1);
        	System.out.println(index+"    "+count);
        	System.out.println(allTuple);
        }
        
        //System.out.println(index+"    "+num);
        else
        {
        	switch(what)
            {
        	case COUNT:
        		if(!allTuple.containsKey(index))
            	{
            		allTuple.put(index, 1);
            		System.out.println("COUNT");
            	}
            	else
            	{
            		Integer before=allTuple.get(index);
            		count=before+1;
            	//Integer after=before+num;
            	allTuple.put(index, count);
            	System.out.println(index+"    "+count+"COUNT");
            	
            	}
        		System.out.println(allTuple);
        		//System.out.println(allTuple);
        		return;
			/*
			 * case SUM: if(!allTuple.containsKey(index)) { allTuple.put(index, num);
			 * System.out.println("SUM"); } else { Integer before=allTuple.get(index);
			 * Integer after=before+num; allTuple.put(index, after);
			 * System.out.println("SUM");
			 * 
			 * }return; case AVG: if(!allTuple.containsKey(index)) { allTuple.put(index,
			 * num); System.out.println("AVG"); } else { Integer before=allTuple.get(index);
			 * Integer after=(before+num)/2; allTuple.put(index, after);
			 * System.out.println("AVG"); }return; case MIN:
			 * if(!allTuple.containsKey(index)) { allTuple.put(index, num);
			 * System.out.println("AVG"); } else { Integer before=allTuple.get(index);
			 * Integer after=Math.min(before,num);
			 * 
			 * allTuple.put(index, after); System.out.println("AVG"); }return; case MAX:
			 * if(!allTuple.containsKey(index)) { allTuple.put(index, num);
			 * System.out.println("AVG"); } else { Integer before=allTuple.get(index);
			 * Integer after=Math.max(before,num); allTuple.put(index, after);
			 * System.out.println("AVG"); }return;
			 */
			default:
				break;
            	
                
            	//Field a=allTuple.get();
            	//System.out.println(allTuple);
            	
            }
        }
        
        
        //allTuple.put(index, num);
        
        
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor.
	 */
	public OpIterator iterator() {
		ArrayList<Tuple> tuples = new ArrayList<>();

		for (Map.Entry<Field, Integer> example : allTuple.entrySet()) {
			Tuple t = new Tuple(td);
			if (gbfield == Aggregator.NO_GROUPING) {
				// t.setField(0, example.getKey());
				t.setField(0, new IntField(example.getValue()));
			} else {
				t.setField(0, example.getKey());
				t.setField(1, new IntField(example.getValue()));
			}

			tuples.add(t);
			// return (OpIterator) allTuple.entrySet().iterator();
		}
		// return new TupleIterator(td, tuples);
		// return (OpIterator) tuples.iterator();
		return new TupleIterator(td, tuples);
		// some code goes here
		// throw new UnsupportedOperationException("please implement me for lab2");
	}

}
