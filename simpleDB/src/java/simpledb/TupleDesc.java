package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
	private ArrayList<TDItem> tditem;
	private int numFields;
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        //TupleDesc tupledesc=new TupleDesc(typeAr, fieldAr);
    	// some code goes here
    	int fieldArNum=fieldAr.length;
    	tditem = new ArrayList<TDItem>();
    	for(int i=0;i<fieldArNum;i++)
    	{
    		tditem.add(new TDItem(typeAr[i],fieldAr[i]));
    	}
    	numFields=fieldArNum;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	//TupleDesc tupledesc=new TupleDesc(typeAr);
    	// some code goes here// some code goes here
    	int typeArNum=typeAr.length;
    	tditem = new ArrayList<TDItem>();
    	for(int i=0;i<typeArNum;i++)
    	{
    		tditem.add(new TDItem(typeAr[i],"undefined"));
    	}
    	numFields=typeArNum;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        TDItem t=tditem.get(i);
    	return t.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	TDItem t=tditem.get(i);
    	return t.fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name==null)throw new NoSuchElementException("no such element!");
    	int num=0;
    	for(int i=0;i<numFields;i++)
        {
        	if(this.getFieldName(i).equals(name))
        	{
        		num=i; return num;
        	}
        }
    	// some code goes here
    	throw new NoSuchElementException("no such element!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size=0;
        for(int i=0;i<numFields;i++)
        {
        	size=size+this.getFieldType(i).getLen();
        }
    	// some code goes here
        //System.out.println(size+"=size");
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int tempNumFields=td1.numFields+td2.numFields;
    	ArrayList<TDItem> td=new ArrayList<TDItem>();
    	
    	Type typeAll[]=new Type[td1.numFields+td2.numFields];
    	String fieldAll[]=new String[td1.numFields+td2.numFields];
    	TupleDesc temp=new TupleDesc(typeAll, fieldAll);
    	for(int i=0;i<tempNumFields;i++)
    	{if(i<td1.numFields)
    	{
    		typeAll[i]=td1.getFieldType(i);
    		fieldAll[i]=td1.getFieldName(i);
    		
    	}else
    	{
    		typeAll[i]=td2.getFieldType(i-td1.numFields);
    		fieldAll[i]=td2.getFieldName(i-td1.numFields);
    	}
    		
    	}
    	//tditem = new ArrayList<TDItem>();
    	//for(int i=0;i<typeArNum;i++)
    	// some code goes here
        return new TupleDesc(typeAll,fieldAll);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	//if(o.equals(null)) return false;
    	if(o==null)return false;
    	if(o.getClass()!=TupleDesc.class) return false;
        TupleDesc temp=(TupleDesc)o;
        if(temp.numFields!=this.numFields) return false;
        for(int i=0;i<numFields;i++)
        {
        	if(this.getFieldType(i)!=temp.getFieldType(i))
        		return false;
        	
        }
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
