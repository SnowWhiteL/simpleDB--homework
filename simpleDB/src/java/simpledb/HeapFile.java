package simpledb;

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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	private File f;
	private TupleDesc td;
	private int id;
    public HeapFile(File f, TupleDesc td) {
        this.f=f;
        this.td=td;
        
    	// some code goes here
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        id=f.getAbsoluteFile().hashCode();// some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {///////////////////////////////////////
        // some code goes here
    	/**
         * Read the specified page from disk.
         *
         * @throws IllegalArgumentException if the page does not exist in this file.
         */
    	Page page=null;
    	byte[] data=new byte[BufferPool.getPageSize()];
    	
    	int t=pid.getTableId();
    	int p=pid.getPageNumber();
    	try {
			InputStream is=new FileInputStream(f);
			int location=p*BufferPool.getPageSize();
			try {
				is.skip(location);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				is.read(data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			page=new HeapPage((HeapPageId) pid,data);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	//Table t=
        return page;
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
        int numpage=(int) (f.length()/BufferPool.getPageSize());
        // some code goes here
        return numpage;
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
        DbFileIterator iterator=new DbFileIterator() {
/*        	������Ҫʵ�ָ� HeapFile.iterator()�������÷���Ӧ����HeapFile��ÿ��ҳ���Ԫ�顣
 * ����������ʹ��BufferPool.getPage()���������е�ҳ�� HeapFile���˷�����ҳ����ص�������У�
 * �����գ����Ժ����Ŀ�У�������ʵ�ֻ��������Ĳ������ƺͻָ�����Ҫ��open���������н���������ص��ڴ���-
 * �⽫���·ǳ���ı�����ڴ治�����
*/        	private Iterator<Tuple> tupleIterator;
			private int pgNo;
			@Override
			public void open() throws DbException, TransactionAbortedException {
				pgNo=0;
				HeapPageId pid=new HeapPageId(getId(), pgNo);
				HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
				tupleIterator=page.iterator();
				// TODO Auto-generated method stub
				
			}

			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if(tupleIterator==null) return false;
				if(tupleIterator.hasNext()) return true;//һҳδ���ʱ
				
				// TODO Auto-generated method stub
				if (pgNo < numPages() - 1) {////////////////////////////��һҳ����
					pgNo++;
	                HeapPageId pid = new HeapPageId(getId(), pgNo);
	                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
	                tupleIterator = page.iterator();
	                //��ʱ����ֱ��return ture���п��ܷ��ص��µĵ������ǲ�����tuple��
	                return tupleIterator.hasNext();
	            } else return false;
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				// TODO Auto-generated method stub
				if(!hasNext()) throw new NoSuchElementException("No Such ElementException");
				else return tupleIterator.next();
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				// TODO Auto-generated method stub
				open();
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub
				pgNo=0;
				tupleIterator=null;
			}
        	
        };
    	// some code goes here
        return iterator ;
    }

}

