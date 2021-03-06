package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    private final File file;
    private final TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
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
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageno = pid.getPageNumber();
        byte[] buffer = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek(BufferPool.getPageSize() * pageno);
            int bytesread = raf.read(buffer);
            raf.close();
            if (bytesread == buffer.length) {
                HeapPageId hpi = new HeapPageId(pid.getTableId(), pid.getPageNumber());
                return new HeapPage(hpi, buffer);
            }
            throw new RuntimeException("Could not read entire page");
        } catch (IOException e) {
            throw new IllegalArgumentException();
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
        return (int) (this.file.length() / (long) BufferPool.getPageSize());
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
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private boolean isOpen;
        private final TransactionId tid;
        private int pageNo;
        private Iterator<Tuple> pageIterator;
        private PageId curPageId;
        private Permissions perm;
        private int tableId;

        HeapFileIterator(TransactionId tid) {
            this.isOpen = false;
            this.tid = tid;
            this.perm = Permissions.READ_ONLY;
            this.tableId = getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen = true;
            this.rewind();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.isOpen) {
                return false;
            }
            if (this.pageIterator.hasNext()) {
                return true;
            } else {

                // Try to load the next page
                this.pageNo++;
                if (this.pageNo < numPages()) {
                    this.curPageId = new HeapPageId(this.tableId, pageNo);
                    HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(this.tid, curPageId, this.perm);
                    this.pageIterator = curPage.iterator();

                    // Note: cannot call this.pageIterator.hasNext(), because there
                    // could be one more empty page
                    return hasNext();
                } else {
                    return false;
                }
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                Tuple t = this.pageIterator.next();
                if (t == null) {
                    System.out.println("Null");
                }

                return t;
            } else {
                throw new NoSuchElementException("No more elements");
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pageNo = 0;
            this.curPageId = new HeapPageId(this.tableId, pageNo);
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(this.tid, this.curPageId, this.perm);
            this.pageIterator = curPage.iterator();
        }

        @Override
        public void close() {
            this.isOpen = false;
        }
    }

}

