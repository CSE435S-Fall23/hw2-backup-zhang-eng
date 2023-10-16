package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 * 
 * 
 *
 */
public class HeapFile {
	
	//HeapFile equivalent to table
	
	public static final int PAGE_SIZE = 4096;
	
	private File file;
	private TupleDesc type;
	
	/**
	 * Can keep extending 
	 * 
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		this.file = f;
		this.type = type;
	}
	
	public File getFile() {
		return file;
	}
	
	public TupleDesc getTupleDesc() {
		return type;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		byte[] byteStream = new byte[PAGE_SIZE];
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(file, "r");
			raf.seek(PAGE_SIZE * id);
			raf.readFully(byteStream);
			raf.close();
			
			return new HeapPage(id, byteStream, this.getId());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		return this.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) throws Exception {
		assert p instanceof HeapPage : "Writes the given HeapPage to disk.";
	    RandomAccessFile raf = new RandomAccessFile(file, "rw");
	    raf.seek(PAGE_SIZE * p.getId());
	    raf.write(p.getPageData());
	    raf.close();
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) throws Exception {
		if (!type.equals(t.getDesc())) {
			throw new Exception("Given tuple does not have the appropriate TupleDesc.");
		}
        
        for (int curPage = 0; curPage < getNumPages(); curPage++) {
        	HeapPage hp = readPage(curPage);
        	for (int curSlot = 0; curSlot < hp.getNumSlots(); curSlot++) {
                if (!hp.slotOccupied(curSlot)) {
                	//first open slot
                	hp.addTuple(t);
                	try {
	                    byte[] byteStream = hp.getPageData();
	                    RandomAccessFile raf = new RandomAccessFile(file, "rw");
	                    raf.seek(PAGE_SIZE * curPage);
	                    raf.write(byteStream);
	                    raf.close();
	                }
	                catch (IOException e) {
	                    throw e;
	                }
                	return hp;
                }
            }
        }
        //new page if all all others are full
        HeapPage hp = new HeapPage(getNumPages(), new byte[PAGE_SIZE], this.getId());
        hp.addTuple(t);
        this.writePage(hp);
        return hp;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t) throws Exception {
		if (!type.equals(t.getDesc())) {
			throw new Exception("Given tuple does not have the appropriate TupleDesc.");
		}
        
        for (int curPage = 0; curPage < getNumPages(); curPage++) {
        	HeapPage hp = readPage(curPage);
        	for (int curSlot = 0; curSlot < hp.getNumSlots(); curSlot++) {
                if (hp.slotOccupied(curSlot)) {
                	hp.deleteTuple(t);
                	try {
	                    byte[] byteStream = hp.getPageData();
	                    RandomAccessFile raf = new RandomAccessFile(file, "rw");
	                    raf.seek(PAGE_SIZE * curPage);
	                    raf.write(byteStream);
	                    raf.close();
	                }
	                catch (IOException e) {
	                    throw e;
	                }
                }
            }
        }
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		
        for (int i = 0; i < getNumPages(); i ++) {
        	HeapPage hp = readPage(i);
        	Iterator<Tuple> tupIterator = hp.iterator();
    		while (tupIterator.hasNext()) {
    			tuples.add(tupIterator.next());
    		}
        }
        return tuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		return (int) Math.ceil(file.length()/PAGE_SIZE);
	}
}
