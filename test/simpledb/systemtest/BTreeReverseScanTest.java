package simpledb.systemtest;

import org.junit.Test;
import simpledb.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BTreeReverseScanTest extends SimpleDbTestBase {
    private final static Random r = new Random();

    /**
     * Tests the scan operator for a table with the specified dimensions.
     */
    private void validateScan(int[] columnSizes, int[] rowSizes)
            throws IOException, DbException, TransactionAbortedException {
        TransactionId tid = new TransactionId();
        for (int columns : columnSizes) {
            int keyField = r.nextInt(columns);
            for (int rows : rowSizes) {
                ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
                BTreeFile f = BTreeUtility.createRandomBTreeFile(columns, rows, null, tuples, keyField);
                BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", null);
                SystemTestUtil.matchTuples(reverseScan, tuples);
                Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
            }
        }
        Database.getBufferPool().transactionComplete(tid);
    }

    // comparator to sort Tuples by key field
    private static class TupleComparator implements Comparator<ArrayList<Integer>> {
        private int keyField;

        public TupleComparator(int keyField) {
            this.keyField = keyField;
        }

        public int compare(ArrayList<Integer> t1, ArrayList<Integer> t2) {
            int cmp = 0;
            if (t1.get(keyField) < t2.get(keyField)) {
                cmp = -1;
            } else if (t1.get(keyField) > t2.get(keyField)) {
                cmp = 1;
            }
            return cmp;
        }
    }

    /**
     * Counts the number of readPage operations.
     */
    class InstrumentedBTreeFile extends BTreeFile {
        public InstrumentedBTreeFile(File f, int keyField, TupleDesc td) {
            super(f, keyField, td);
        }

        @Override
        public Page readPage(PageId pid) throws NoSuchElementException {
            readCount += 1;
            return super.readPage(pid);
        }

        public int readCount = 0;
    }

    /**
     * Scan 1-4 columns.
     */
    @Test
    public void testSmall() throws IOException, DbException, TransactionAbortedException {
        int[] columnSizes = new int[]{1, 2, 3, 4};
        int[] rowSizes =
                new int[]{0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + r.nextInt(4096)};
        validateScan(columnSizes, rowSizes);
    }

    /**
     * Test that rewinding a BTreeReverseScan iterator works.
     */
    @Test
    public void testRewind() throws IOException, DbException, TransactionAbortedException {
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = r.nextInt(2);
        BTreeFile f = BTreeUtility.createRandomBTreeFile(2, 1000, null, tuples, keyField);
        Collections.sort(tuples, new BTreeReverseScanTest.TupleComparator(keyField));

        TransactionId tid = new TransactionId();
        BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", null);
        reverseScan.open();
//        System.out.println(tuples.size());
        for (int i = tuples.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }

        reverseScan.rewind();
        for (int i = tuples.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }
        reverseScan.close();
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Test that rewinding a BTreeReverseScan iterator works with predicates.
     */
    @Test
    public void testRewindPredicates() throws IOException, DbException, TransactionAbortedException {
        // Create the table
        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = r.nextInt(3);
        BTreeFile f = BTreeUtility.createRandomBTreeFile(3, 1000, null, tuples, keyField);
        Collections.sort(tuples, new BTreeReverseScanTest.TupleComparator(keyField));

        // EQUALS
        TransactionId tid = new TransactionId();
        ArrayList<ArrayList<Integer>> tuplesFiltered = new ArrayList<ArrayList<Integer>>();
        IndexPredicate ipred = new IndexPredicate(Predicate.Op.EQUALS, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        Iterator<ArrayList<Integer>> it = tuples.iterator();
        while (it.hasNext()) {
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        reverseScan.open();
        for (int i = tuplesFiltered.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }

        reverseScan.rewind();
        for (int i = tuplesFiltered.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }
        reverseScan.close();

        // LESS_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.LESS_THAN, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while (it.hasNext()) {
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        reverseScan.open();
        for (int i = tuplesFiltered.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }

        reverseScan.rewind();
        for (int i = tuplesFiltered.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }
        reverseScan.close();

        // GREATER_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(BTreeUtility.MAX_RAND_VALUE)));
        it = tuples.iterator();
        while (it.hasNext()) {
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        reverseScan.open();
        for (int i = tuplesFiltered.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }

        reverseScan.rewind();
        for (int i = tuplesFiltered.size() - 1; i >= 0; --i) {
            assertTrue(reverseScan.hasNext());
            Tuple t = reverseScan.next();
            assertEquals(tuplesFiltered.get(i), SystemTestUtil.tupleToList(t));
        }
        reverseScan.close();
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Test that scanning the BTree for predicates does not read all the pages
     */
    @Test
    public void testReadPage() throws Exception {
        // Create the table
        final int LEAF_PAGES = 30;

        ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
        int keyField = 0;
        BTreeFile f = BTreeUtility.createBTreeFile(2, LEAF_PAGES * 502, null, tuples, keyField);
        Collections.sort(tuples, new BTreeReverseScanTest.TupleComparator(keyField));
        TupleDesc td = Utility.getTupleDesc(2);
        BTreeReverseScanTest.InstrumentedBTreeFile table = new BTreeReverseScanTest.InstrumentedBTreeFile(f.getFile(), keyField, td);
        Database.getCatalog().addTable(table, SystemTestUtil.getUUID());

        // EQUALS
        TransactionId tid = new TransactionId();
        ArrayList<ArrayList<Integer>> tuplesFiltered = new ArrayList<ArrayList<Integer>>();
        IndexPredicate ipred = new IndexPredicate(Predicate.Op.EQUALS, new IntField(r.nextInt(LEAF_PAGES * 502)));
        Iterator<ArrayList<Integer>> it = tuples.iterator();
        while (it.hasNext()) {
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) == ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        BTreeReverseScan reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(reverseScan, tuplesFiltered);
        // root pointer page + root + leaf page (possibly 2 leaf pages)
        assertTrue(table.readCount == 3 || table.readCount == 4);

        // LESS_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.LESS_THAN, new IntField(r.nextInt(LEAF_PAGES * 502)));
        it = tuples.iterator();
        while (it.hasNext()) {
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) < ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(reverseScan, tuplesFiltered);//此处table.readCount从0变成了32
        // root pointer page + root + leaf pages
        int leafPageCount = tuplesFiltered.size() / 502;
        if (leafPageCount < LEAF_PAGES)
            leafPageCount++; // +1 for next key locking
        assertEquals(leafPageCount + 2, table.readCount);

        // GREATER_THAN
        tuplesFiltered.clear();
        ipred = new IndexPredicate(Predicate.Op.GREATER_THAN_OR_EQ, new IntField(r.nextInt(LEAF_PAGES * 502)));
        it = tuples.iterator();
        while (it.hasNext()) {
            ArrayList<Integer> tup = it.next();
            if (tup.get(keyField) >= ((IntField) ipred.getField()).getValue()) {
                tuplesFiltered.add(tup);
            }
        }

        Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        table.readCount = 0;
        reverseScan = new BTreeReverseScan(tid, f.getId(), "table", ipred);
        SystemTestUtil.matchTuples(reverseScan, tuplesFiltered);
        // root pointer page + root + leaf pages
        leafPageCount = tuplesFiltered.size() / 502;
        if (leafPageCount < LEAF_PAGES)
            leafPageCount++; // +1 for next key locking
        assertEquals(leafPageCount + 2, table.readCount);

        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Make test compatible with older version of ant.
     */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(BTreeReverseScanTest.class);
    }
}
