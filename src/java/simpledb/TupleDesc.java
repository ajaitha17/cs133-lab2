package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    // Storing the Tuple Fields in an arraylist of tditems
    ArrayList<TDItem> fields = new ArrayList<TDItem>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
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
        return fields.iterator();
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
        int i, l = typeAr.length;
        for(i=0;i<l;i++) {
            fields.add(new TDItem(typeAr[i],fieldAr[i]));
        }
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
        int i, l = typeAr.length;
        for(i=0;i<l;i++) {
            fields.add(new TDItem(typeAr[i],null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fields.size();
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
        int size = this.fields.size();
        if(i > size)
            throw new NoSuchElementException();
        return fields.get(i).fieldName;
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
        int size = fields.size();
        if(i > size)
            throw new NoSuchElementException();
        return fields.get(i).fieldType;
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
        if(name==null)
            throw new NoSuchElementException();
        int i,l = fields.size();
        String fName;
        for(i=0;i<l;i++) {
            fName = fields.get(i).fieldName;
            if(name.equals(fName))
                return i;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sum = 0,l = fields.size(),i;
        for(i=0;i<l;i++) {
            sum += fields.get(i).fieldType.getLen();
        }

        return sum;
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
        int i,l1 = td1.fields.size(), l2 = td2.fields.size();

        Type[] t1 = new Type[l1+l2];
        String[] s1 = new String[l1 + l2];
        for(i=0;i<l1;i++) {
            t1[i] = td1.fields.get(i).fieldType;
            s1[i] = td1.fields.get(i).fieldName;
        }

        for(i=0;i<l2;i++) {
            t1[i + l1] = td2.fields.get(i).fieldType;
            s1[i + l1] = td2.fields.get(i).fieldName;
        }

        TupleDesc td = new TupleDesc(t1,s1);

        return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(o==null)
            return false;
        if(o.equals(new Object()))
            return false;
        try {
            TupleDesc ob = (TupleDesc) o;
            int s1 = this.fields.size(), s2 = ob.fields.size();
            if(s1 != s2)
                return false;
            int i;
            for(i=0;i<s1;i++) {
                TDItem t1 = this.fields.get(i), t2 = ob.fields.get(i);
                if(!t1.fieldType.equals(ob.fields.get(i).fieldType))
                    return false;
            }
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder result = new StringBuilder();
        int l = fields.size(),i;
        
        for(i=0;i<l;i++) {
            result.append(fields.get(i).toString());
        }

        return result.toString();
    }
}
