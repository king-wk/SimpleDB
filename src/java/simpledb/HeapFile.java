package simpledb;

import javax.swing.text.html.HTMLDocument;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    private int numpages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.numpages = (int) f.length() / BufferPool.DEFAULT_PAGE_SIZE;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getTableId() != getId()) {
            return null;
        } else {
            int pos = pid.getPageNumber() * BufferPool.DEFAULT_PAGE_SIZE;
            File file = getFile();
            Page page = null;
            byte[] data = new byte[BufferPool.DEFAULT_PAGE_SIZE];
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");//RandomAccessFile可以自由访问文件的任意位置
                raf.seek(pos);//将记录指针定位到pos位置
                raf.read(data, 0, data.length);//读一个page长度，存入data中
                page = new HeapPage((HeapPageId) pid, data);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return page;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numpages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private int index;
        private Iterator<Tuple> TuplesInPage;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        private Iterator<Tuple> GetTuplesInPage(HeapPageId pid) throws DbException, TransactionAbortedException {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            HeapPageId pid = new HeapPageId(getId(), index);
            TuplesInPage = GetTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (TuplesInPage == null) {
                return false;
            } else {
                if (TuplesInPage.hasNext()) {
                    return true;
                } else {
                    if (index < numPages() - 1) {
                        HeapPageId pageId = new HeapPageId(getId(), index);
                        TuplesInPage = GetTuplesInPage(pageId);
                        return TuplesInPage.hasNext();
                    } else return false;
                }
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                index++;
                return TuplesInPage.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            index = 0;
            TuplesInPage = null;
        }
    }

}

