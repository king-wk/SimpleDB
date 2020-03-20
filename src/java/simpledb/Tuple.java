package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    private TupleDesc tupleDesc;
    private Field[] fields;
    //tuple在磁盘中的位置
    private RecordId recordId;
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        fields = new Field[td.numFields()];//初始化fields数组
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i >= fields.length) {//如果索引值不合法抛出异常
            throw new IllegalArgumentException();
        }
        this.fields[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= fields.length) {//如果索引值不合法抛出异常
            throw new IllegalArgumentException();
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuffer string = new StringBuffer();
        for (int i = 0; i < fields.length; i++) {
            string.append(fields[i].toString());
            string.append("\t");
        }
        return string.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        if (fields.length > 0) {
            return new Iterator<Field>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    //如果当前索引小于字段的数量，那么就有下一个字段
                    return index < fields.length;
                }

                @Override
                public Field next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    //返回索引值对应的字段并且将索引往后移一位
                    return fields[index++];
                }
            };
        } else return null;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
    }
}
