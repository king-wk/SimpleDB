package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private TupleDesc tupleDesc;
    private Tuple tuple1;
    private Tuple tuple2;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        // some code goes here
        return tupleDesc.getFieldName(p.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        // some code goes here
        return tupleDesc.getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        super.open();
        child1.rewind();
        child2.rewind();
        // some code goes here
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (tuple1 == null && child1.hasNext())
            tuple1 = child1.next();
        while (tuple1 != null) {
            while (child2.hasNext()) {
                tuple2 = child2.next();
                if (p.filter(tuple1, tuple2)) {
                    Tuple newTuple = new Tuple(getTupleDesc());
                    int i = 0;
                    Iterator<Field> iterator = tuple1.fields();
                    while (iterator.hasNext()) {
                        newTuple.setField(i++, iterator.next());
                    }
                    iterator = tuple2.fields();
                    while (iterator.hasNext()) {
                        newTuple.setField(i++, iterator.next());
                    }
                    return newTuple;
                }
            }
            child2.rewind();
            if (child1.hasNext()) {
                tuple1 = child1.next();
            } else return null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        //if (children.length != 2) return;
        child1 = children[0];
        child2 = children[1];
    }

}
