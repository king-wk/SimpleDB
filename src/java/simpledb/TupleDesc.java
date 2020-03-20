package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private List<TDItem> items;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
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
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        if (items.size() > 0) {
            return new Iterator<TDItem>() {
                private Integer index = 0;

                @Override
                public boolean hasNext() {
                    //如果当前索引小于items的数量，那么就有下一个字段
                    return index < items.size();
                }

                @Override
                public TDItem next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    //返回索引值对应的字段并且将索引往后移一位
                    return items.get(index++);
                }
            };
        } else return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        //如果type数组和field数组长度不等或者数组为空，抛出异常
        if (typeAr.length != fieldAr.length || typeAr.length <= 0) {
            throw new IllegalArgumentException();
        }
        items = new ArrayList<>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            items.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length <= 0) {//如果数组为空，抛出异常
            throw new IllegalArgumentException();
        }
        items = new ArrayList<>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], null);
            items.add(item);
        }
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields() || i < 0) {//如果查询索引小于0或大于等于fields的长度，抛出异常
            throw new NoSuchElementException();
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields() || i < 0) {//如果查询索引小于0或大于等于fields的长度，抛出异常
            throw new NoSuchElementException();
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null)
            throw new NoSuchElementException();
        for (int i = 0; i < items.size(); i++) {
            if (name.equals(items.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sum = 0;
        for (int i = 0; i < items.size(); i++) {
            sum += getFieldType(i).getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int length1 = td1.items.size();
        int length2 = td2.items.size();
        Type[] typeAr = new Type[length1 + length2];
        String[] fieldAr = new String[length1 + length2];
        for (int i = 0; i < length1; i++) {
            typeAr[i] = td1.items.get(i).fieldType;
            fieldAr[i] = td1.items.get(i).fieldName;
        }
        for (int i = 0; i < length2; i++) {
            typeAr[i + length1] = td2.items.get(i).fieldType;
            fieldAr[i + length1] = td2.items.get(i).fieldName;
        }
        //合并两个TupleDesc的fieldtype和fieldname，感觉有些繁琐
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        //如果o是null抛出异常
        if (o == null) {
            return false;
        }
        //判断o是否是TupleDesc的实现类
        if (o instanceof TupleDesc) {
            //强制类型转换
            TupleDesc another = (TupleDesc) o;
            //判断两个TupleDesc是否有相同数量的items
            if (another.numFields() == this.numFields()) {
                for (int i = 0; i < numFields(); i++) {
                    //判断两个TupleDesc对应的每一个item的fieldtype是否相同
                    if (!another.items.get(i).fieldType.
                            equals(items.get(i).fieldType)) {
                        return false;
                    }
                }
            } else return false;
            return true;
        } else return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return numFields() * 8;//随便写的好像不影响啥
        //throw new UnsupportedOperationException("unimplemented");
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
        StringBuffer string = new StringBuffer();
        for (int i = 0; i < items.size(); i++) {
            string.append(items.get(i).toString() + ",");
        }
        return string.toString();
    }
}
