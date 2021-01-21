package simpledb;

import javax.management.RuntimeErrorException;
import java.io.*;
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


    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if(getId() == pid.getTableId())
        {
            int pgNo = pid.getPageNumber();

            if(pgNo>=0 && pgNo<numPages())
            {
                byte[] bytes = HeapPage.createEmptyPageData();

                try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                    raf.seek((long) BufferPool.getPageSize() * pid.getPageNumber());
                    raf.read(bytes, 0, BufferPool.getPageSize());
                    return new HeapPage((HeapPageId) pid, bytes);
                }
                catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        }
        throw new IllegalArgumentException("no such page!");
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
        return (int)(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        BufferPool pool = Database.getBufferPool();
        ArrayList<Page> pages = new ArrayList<>();
        int tableId = getId();
        int pid = 0;
        for(; pid<numPages();++pid)
        {
            HeapPage pg = (HeapPage) pool.getPage(tid, new HeapPageId(tableId, pid), Permissions.READ_WRITE);
            if(pg.getNumEmptySlots()>0)
            {
                pg.insertTuple(t);
                pages.add(pg);
                break;
            }
        }
        if (pid == numPages())
        {
            HeapPageId newId = new HeapPageId(tableId, pid);
            HeapPage newPage = new HeapPage(newId, HeapPage.createEmptyPageData());
            writePage(newPage);
            //通过bufferpool访问
            HeapPage pg = (HeapPage) pool.getPage(tid, newId, Permissions.READ_WRITE);
            pg.insertTuple(t);
            pages.add(pg);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        BufferPool pool = Database.getBufferPool();
        ArrayList<Page> pages = new ArrayList<>();
        int tableId = getId();
        PageId pageid = t.getRecordId().getPageId();
        if(pageid.getPageNumber()>numPages())
            throw new DbException("No such tuple!");
        HeapPage pg = (HeapPage) pool.getPage(tid, pageid, Permissions.READ_WRITE);
        pg.deleteTuple(t);
        pages.add(pg);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {

            private final BufferPool pool = Database.getBufferPool();
            private final int tableId = getId();
            private int pid = -1;
            private Iterator<Tuple> child;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pid = 0;
                child = null;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (null != child && child.hasNext()) {
                    return true;
                } else if (pid < 0 || pid >= numPages()) {
                    return false;
                } else {
                    child = ((HeapPage)pool.getPage(tid, new HeapPageId(tableId,pid++),
                            Permissions.READ_ONLY)).iterator();
                    return hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext())
                    throw new NoSuchElementException();
                else
                    return child.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pid = -1;
                child = null;
            }
        };
    }

}

