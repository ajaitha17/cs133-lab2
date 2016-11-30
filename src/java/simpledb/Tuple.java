package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    RecordId rID;
    ArrayList<Field> fields;
    TupleDesc tupledesc;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        int l = td.fields.size();
        fields = new ArrayList<Field>(l);
        // for(i=0;i<l;i++) {
        //     Type t = td.fields.get(i).fieldType;
        //     if(t.equals(Type.INT_TYPE)) {
        //         fields.add(new IntField());
        //     }
        //     else fields.add(new StringField());
        // }
        tupledesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupledesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {        
        return this.rID;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rID = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if(i < fields.size())
            fields.set(i,f);
        else fields.add(f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int l = fields.size(),i;
        for(i=0;i<l-1;i++) {
            sb.append(fields.get(i).toString());
            sb.append("\t");
        }
        sb.append(fields.get(l-1).toString());
        sb.append("\n");
        return sb.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fields.iterator();
    }
    
    /**
     * Reset the TupleDesc of this tuple
     * Does not need to worry about the fields inside the Tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupledesc = td;
    }
}
