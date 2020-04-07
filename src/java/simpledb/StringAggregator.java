package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupValue;
    private HashMap<Field, List<Tuple>> groupTuple;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupValue = new HashMap<>();
        groupTuple = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField = null;
        if (gbfield != -1) {
            gField = tup.getField(gbfield);
        }
        List<Tuple> tuples = groupTuple.get(gField);
        if (tuples == null) {
            tuples = new ArrayList<>();
        }
        tuples.add(tup);
        groupTuple.put(gField, tuples);
        if (tup.getField(afield).getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException();
        }
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        groupValue.put(gField, groupTuple.get(gField).size());
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> k : groupValue.entrySet()) {
            Tuple tuple;
            //分别处理分组和不分组的情况
            if (gbfield == Aggregator.NO_GROUPING) {
                List<Type> temp = new ArrayList<>();
                temp.add(Type.INT_TYPE);
                tuple = new Tuple(new TupleDesc(temp.toArray(new Type[0])));
                tuple.setField(0, new IntField(k.getValue()));
            } else {
                List<Type> temp = new ArrayList<>();
                temp.add(gbfieldtype);
                temp.add(Type.INT_TYPE);
                tuple = new Tuple(new TupleDesc(temp.toArray(new Type[0])));
                tuple.setField(0, k.getKey());
                tuple.setField(1, new IntField(k.getValue()));
            }
            tuples.add(tuple);
        }
        if (tuples.size() == 0) {
            return new TupleIterator(new TupleDesc(new Type[]{gbfieldtype}), tuples);
        }
        return new TupleIterator(tuples.get(0).getTupleDesc(), tuples);
    }

}
