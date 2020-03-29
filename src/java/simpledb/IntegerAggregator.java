package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    //索引指定了要使用tuple的哪一个列来分组
    private int gbfield;
    //指定作为分组依据的那一列的值的类型
    private Type gbfieldtype;
    //索引指定了要使用tuple的哪一个列来聚合
    private int afield;
    //聚合操作
    private Op what;
    //这个map仅用于辅助计算平均值
    private Map<Field, List<Tuple>> groupTuple;
    //Key：每个不同的分组字段(groupby value)  Vlaue：聚合的结果
    private Map<Field, Integer> groupValue;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupValue = new HashMap<>();
        groupTuple = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
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
        if (tup.getField(afield).getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException();
        }
        int tupValue;
        switch (what) {
            case COUNT:
                groupValue.put(gField, groupTuple.get(gField).size());
                break;
            case SUM:
                int sumValue = groupValue.get(gField) == null ? 0 : groupValue.get(gField);
                sumValue += ((IntField) tup.getField(afield)).getValue();
                groupValue.put(gField, sumValue);
                break;
            case MIN:
                int min = groupValue.get(gField) == null ? Integer.MAX_VALUE : groupValue.get(gField);
                tupValue = ((IntField) tup.getField(afield)).getValue();
                min = Math.min(min, tupValue);
                groupValue.put(gField, min);
                break;
            case MAX:
                int max = groupValue.get(gField) == null ? Integer.MIN_VALUE : groupValue.get(gField);
                tupValue = ((IntField) tup.getField(afield)).getValue();
                max = Math.max(max, tupValue);
                groupValue.put(gField, max);
                break;
            case AVG:
                int result = 0;
                for (int i = 0; i < tuples.size(); i++) {
                    result += ((IntField) tuples.get(i).getField(afield)).getValue();
                }
                groupValue.put(gField, result / tuples.size());
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> k : groupValue.entrySet()) {
            Tuple tuple;
            //分别处理分组和不分组的情况
            if (gbfield == Aggregator.NO_GROUPING) {
                List<Type> temp = new ArrayList<>();
                temp.add(gbfieldtype);
                tuple = new Tuple(new TupleDesc(temp.toArray(new Type[0])));
                tuple.setField(0, new IntField(k.getValue()));
                System.out.println(temp.get(0));
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
