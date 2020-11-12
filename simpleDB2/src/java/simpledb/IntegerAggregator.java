package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private HashMap<Field, Integer> allTuple;
	private HashMap<Field, Integer[]> avgTuple;
	private TupleDesc td;
	//private int num_AVG;

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		allTuple = new HashMap<Field, Integer>();
		avgTuple = new HashMap<Field, Integer[]>();
		//num_AVG = 0;
		// some code goes here
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		System.out.println(tup.toString());
		if (gbfield == Aggregator.NO_GROUPING) {
			Type[] type = new Type[1];
			String[] fieldAr = new String[1];
			type[0] = tup.getTupleDesc().getFieldType(afield);
			fieldAr[0] = tup.getTupleDesc().getFieldName(afield);
			td = new TupleDesc(type, fieldAr);
		} else {
			Type[] type = new Type[2];
			String[] fieldAr = new String[2];
			type[0] = gbfieldtype;
			fieldAr[0] = tup.getTupleDesc().getFieldName(gbfield);
			type[1] = gbfieldtype;
			fieldAr[1] = tup.getTupleDesc().getFieldName(afield);
			td = new TupleDesc(type, fieldAr);
		}
		// td=tup.getTupleDesc();
		Field index;
		if (gbfield == Aggregator.NO_GROUPING) {
			index = null;
		} else
			index = tup.getField(gbfield);

		// if(tup==null) allTuple.clear();

		Integer num;
		if ((tup.getField(afield)) == null)
			num = 0;
		else
			num = ((IntField) tup.getField(afield)).getValue();

		if (allTuple.isEmpty()) {
			//num_AVG++;
			System.out.println("ssss" + what);
			if (what.equals(Op.COUNT)) {
				allTuple.put(index, 1);
			} else {
				allTuple.put(index, num);
				avgTuple.put(index, new Integer[]{1,num});
				//avgTuple.put(index, new Integer[] {num});
			}
			
		}
		// System.out.println(index+" "+num);
		else {
			switch (what) {
			case COUNT:
				if (!allTuple.containsKey(index)) {// num_AVG++;
					allTuple.put(index, 1);
					
					// System.out.println("SUM");
				} else {
					Integer before = allTuple.get(index);
					// num_AVG++;
					Integer after = before + 1;

					allTuple.put(index, after);
					// System.out.println("SUM");

				}
				return;
			case SUM:
				if (!allTuple.containsKey(index)) {
					allTuple.put(index, num);
					// System.out.println("SUM");
				} else {
					Integer before = allTuple.get(index);
					Integer after = before + num;
					allTuple.put(index, after);

					// System.out.println("SUM");

				}
				return;
			case AVG:
				//num_AVG++;
				if (!allTuple.containsKey(index)) {
					allTuple.put(index, num);
					avgTuple.put(index, new Integer[]{1,num});
					
					
				} else {
					Integer avgsum = avgTuple.get(index)[0];
					Integer before = avgTuple.get(index)[1];
					Integer after=(before+num)/(avgsum+1);
					allTuple.put(index, after);
					//Integer after = (before * (avgTuple.get(index)) + num) / (avgTuple.get(index)+1);
					avgTuple.put(index, new Integer[] {avgsum+1,before+num});
					
					allTuple.put(index, after);
					
					// System.out.println("AVG");
				}
				return;
			case MIN:
				if (!allTuple.containsKey(index)) {
					allTuple.put(index, num);
					// System.out.println("AVG");
				} else {
					Integer before = allTuple.get(index);
					Integer after = Math.min(before, num);

					allTuple.put(index, after);
					// System.out.println("AVG");
				}
				return;
			case MAX:
				if (!allTuple.containsKey(index)) {
					allTuple.put(index, num);
					// System.out.println("AVG");
				} else {
					Integer before = allTuple.get(index);
					Integer after = Math.max(before, num);
					allTuple.put(index, after);
					// System.out.println("AVG");
				}
				return;

			// Field a=allTuple.get();
			/// System.out.println(allTuple);

			}
		}

		// allTuple.put(index, num);

		// System.out.println(allTuple);

		// some code goes here
	}

	/*
	 * public void calculateGroupTuple() { switch(what) { case SUM: //Field
	 * a=allTuple.get(); //System.out.println(what.toString()+"a"+a);
	 * 
	 * } }
	 */
	/**
	 * Create a OpIterator over group aggregate results.
	 * 
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor.
	 */
	public OpIterator iterator() {
		// some code goes here

		// TupleDesc td = null;
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
		/*
		 * throw new UnsupportedOperationException("please implement me for lab2");
		 */
	}

}
