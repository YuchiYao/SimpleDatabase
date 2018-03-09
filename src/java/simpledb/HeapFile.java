package simpledb;

import javax.xml.crypto.Data;
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

    private File file;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
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
        // Table ID
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try{
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());;
            byte[] readData = new byte[BufferPool.getPageSize()];
            raf.read(readData);
            raf.close();
            return new HeapPage((HeapPageId) pid, readData);
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try{
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            int offset = page.getId().getPageNumber();
            raf.seek(offset * BufferPool.getPageSize());;
            raf.write(page.getPageData(),0,BufferPool.getPageSize());
            raf.close();

        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageList = new ArrayList<>();
        for(int i = 0; i < numPages(); i++){
            PageId pageId = new HeapPageId(getId(),i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);

            if(heapPage.getNumEmptySlots()!=0){
                heapPage.insertTuple(t);
                pageList.add(heapPage);
                break;
            }
        }
        if(pageList.size()==0){
            HeapPageId pageId = new HeapPageId(getId(),numPages());
            byte[] data = HeapPage.createEmptyPageData();
            try{
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                raf.seek(pageId.getPageNumber() * BufferPool.getPageSize());
                raf.write(data);
                raf.close();
            } catch(FileNotFoundException e){
                e.printStackTrace();
            } catch(IOException e){
                e.printStackTrace();
            }
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
            heapPage.insertTuple(t);
            pageList.add(heapPage);
        }
        return pageList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageList = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        pageList.add(heapPage);
        return pageList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator{
        private Iterator<Tuple> tupleIterator;
        private int pageIndex;
        private TransactionId tid;
        private int totalPageNo;
        private boolean isOpen;
        public HeapFileIterator(TransactionId tid){
            isOpen = false;
            pageIndex = 0;
            this.tid = tid;
            totalPageNo = numPages();
        }

        private Iterator<Tuple> getTupleIterator(int pageNo) throws DbException, TransactionAbortedException{
            int tableId = getId();
            PageId pid = new HeapPageId(tableId, pageNo);

            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            return heapPage.iterator();
        }
        @Override
        public void open() throws DbException, TransactionAbortedException{
            tupleIterator = getTupleIterator(pageIndex);
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            if(!isOpen){
                return false;
            }
            if(tupleIterator!=null && tupleIterator.hasNext()){
                return true;
            }else{
                pageIndex++;
                while(pageIndex < totalPageNo){
                    tupleIterator = getTupleIterator(pageIndex);
                    if(tupleIterator.hasNext()){
                        return true;
                    }
                    pageIndex++;
                }
                return false;
            }
        }
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if(!isOpen){
                throw new NoSuchElementException("didn't open");
            }
            if(hasNext()){
                return tupleIterator.next();
            }
            throw new NoSuchElementException();
        }
        @Override
        public void rewind() throws DbException, TransactionAbortedException{

            close();
            pageIndex = 0;
            open();
        }

        @Override
        public void close(){

            isOpen = false;
        }
    }

}

