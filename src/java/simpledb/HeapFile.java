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
    //page数量
    //private int numpages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
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
            throw new IllegalArgumentException();
        } else {
            int pos = pid.getPageNumber() * BufferPool.DEFAULT_PAGE_SIZE;//找到page在HeadPage上的偏移量
            File file = getFile();
            Page page = null;
            byte[] data = new byte[BufferPool.DEFAULT_PAGE_SIZE];//用于存要读的page
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");//RandomAccessFile可以自由访问文件的任意位置
                raf.seek(pos);//将记录指针定位到pos位置
                raf.read(data, 0, data.length);//读一个page，存入data中
                page = new HeapPage((HeapPageId) pid, data);
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
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.DEFAULT_PAGE_SIZE);
            byte[] data = page.getPageData();
            raf.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.DEFAULT_PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        //找空闲的slot
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pageId = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() != 0) {
                heapPage.insertTuple(t);
                //heapPage.markDirty(true, tid);
                pages.add(heapPage);
                break;
            }
        }
        //如果page都已经满了
        if (pages.size() == 0) {
            //创建一个新的page
            HeapPageId NewPageId = new HeapPageId(getId(), numPages());
            HeapPage page = new HeapPage(NewPageId, HeapPage.createEmptyPageData());
            page.insertTuple(t);
            pages.add(page);
            //写入磁盘
            writePage(page);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        ArrayList<Page> pages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (i == pageId.getPageNumber()) {
                heapPage.deleteTuple(t);
                //heapPage.markDirty(true, tid);
                pages.add(heapPage);
                break;
            }
        }
        if (pages.size() == 0) {
            throw new DbException("");
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //存储一个当前正在遍历页的tuples迭代器，一页一页的遍历
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
            //不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            //加载第一页的tuples
            HeapPageId pid = new HeapPageId(getId(), index);
            TuplesInPage = GetTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (TuplesInPage == null) {
                //如果遍历页的迭代器为null说明迭代器已经关闭了
                return false;
            } else {
                if (TuplesInPage.hasNext()) {
                    //如果当前页tuples迭代器还没有遍历完
                    return true;
                } else {
                    if (index < numPages() - 1 && index >= 0) {
                        //还没有到达最后一页
                        //不能直接返回true，需要判断下一页是否有tuples可读
                        index++;
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
                //写index++，一直报错
                //这里index不需要再加1了，hasNext()判断的时候已经到了下一个tuple或者下一页的tuple
                return TuplesInPage.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            //重新读一次
            open();
        }

        @Override
        public void close() {
            index = 0;
            TuplesInPage = null;
        }
    }

}

