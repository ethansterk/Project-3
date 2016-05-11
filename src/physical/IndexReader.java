package physical;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import code.DatabaseCatalog;
import code.Tuple;
import code.TupleReader;

/**
 * 
 * The IndexReader class is the index-equivalent of the Tuple-
 * Reader. Initially, it traverses the index root-to-leaf and
 * this initializes the IndexReader to return tuples using a
 * TupleReader. It knows which tuple to look for in the Tuple-
 * Reader by using the the rid in the Leaf nodes.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class IndexReader {

	private String tableName;
	private String alias;
	private String sortAttr;
	private FileChannel fc;
	private ByteBuffer buffer;
	private TupleReader tr;
	private int lowKey;
	private int highKey;
	private boolean clustered;
	private int ROOT_ADDR;
	private int NUM_LEAVES;
	private int ORDER;
	private int currentPage;
	private int dataEntriesLeft;
	private int ridsLeft;
	private boolean inIndex;
	
	/**
	 * Constructs an IndexReader.
	 * @param indexDir The directory of the index file.
	 * @param lowKey The low key (inclusive).
	 * @param highKey The high key (inclusive).
	 * @param clustered True if the index is clustered.
	 */
	public IndexReader(String indexDir, int lowKey, int highKey, boolean clustered, String tableName, String alias, String sortAttr) {
		this.lowKey = lowKey;
		this.highKey = highKey;
		this.clustered = clustered;
		tr = new TupleReader(tableName, alias, false, null, null);
		this.tableName = tableName;
		this.alias = alias;
		this.sortAttr = sortAttr;
		inIndex = true;
		//retrieve the file channel
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(indexDir);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fin.getChannel();
		
		buffer = ByteBuffer.allocate(4096);
		
		handleFirstDescent();
		
	}
	
	/**
	 * Performs the first root-to-leaf traversal of the B+-tree, 
	 * initializing the first data entry so that the next call
	 * to getNextTuple() can get the appropriate tuple without
	 * another root-to-leaf traversal.
	 */
	private void handleFirstDescent() {
		//go into header
		try {
			if (fc.read(buffer) < 1) {
				System.out.println("ERR: reached end of FileChannel");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer.position(0);
		// root in header file
		ROOT_ADDR = buffer.getInt();
		// store number of leaves
		NUM_LEAVES = buffer.getInt();
		// store the order of the tree
		ORDER = buffer.getInt();
		
		//go into root
		setBufferToAddress(ROOT_ADDR);
		
		// loop until you reach leaf node (different serialization)
		while(buffer.getInt() == 1) {
			int nextPageAddr = -1;
			int numKeys = buffer.getInt();
			for(int i = 0; i < numKeys; i++) {
				int key = buffer.getInt();
				if(lowKey <= key) {
					int oldPos = buffer.position();
					if(oldPos + (numKeys - 1) * 4 > 4095) {
						System.out.println("Not enough room in buffer.");
					}
					buffer.position(oldPos + (numKeys - 1) * 4);
					nextPageAddr = buffer.getInt();
					break;
				}
			}
			if(nextPageAddr == -1) { // lowKey greater than all keys (addr never assigned)
				int oldPos = buffer.position();
				if(oldPos + (numKeys) * 4 > 4095) {
					System.out.println("Not enough room in buffer.");
				}
				buffer.position(oldPos + (numKeys) * 4);
				nextPageAddr = buffer.getInt();
			}
			// go to new page
			setBufferToAddress(nextPageAddr);
			currentPage = nextPageAddr;
		}
		// at leaf node, find the key that is greater than or equal to lowKey
		dataEntriesLeft = buffer.getInt();
		int key = -1;
		while(lowKey > (key = buffer.getInt()) && dataEntriesLeft > 0) {
			dataEntriesLeft--;
			int numRids = buffer.getInt();
			int oldPos = buffer.position();
			if(oldPos + (numRids * 2) * 4 > 4095) {
				System.out.println("Not enough room in buffer.");
			}
			buffer.position(oldPos + (numRids * 2) * 4);
		}
		dataEntriesLeft--;
		if (dataEntriesLeft == -1)
			inIndex = false;
		else {
			// at key that is greater than or equal to lowKey
			ridsLeft = buffer.getInt();
			// the buffer is now positioned to return the first rid
			if(clustered) {
				int pageID = buffer.getInt();
				int tupleID = buffer.getInt();
				
				int resetIndex = 0;
				int numAttr = DatabaseCatalog.getInstance().getSchema(tableName).getNumCols();
				int tuplesPerPage = 4088 / (4 * numAttr);
				resetIndex = pageID * tuplesPerPage + tupleID;
				tr.reset(resetIndex);
			}
		}
		
	}

	/**
	 * Reads the next tuple from the relation (according to the index).
	 * If the index is clustered, this is simply reading the next tuple
	 * from the sorted relation file. If it is unclustered, this involves
	 * reading in the next rid, reseting the TupleReader to this position,
	 * and returning next tuple at this position.
	 * @return
	 */
	public Tuple readNextTuple() {
		if (!inIndex)
			return null;
		
		if (clustered) {
			Tuple t = tr.readNextTuple();
			String name;
			if(alias != null)
				name = alias;
			else
				name = tableName;
			if(t == null)
				return null;
			int i = t.getFields().indexOf(name + "." + sortAttr);
			int x = Integer.parseInt(t.getValues().get(i));
			if (x > highKey)
				return null;
			return t;
		}
		//else.. 
		if (ridsLeft < 1) {
			if(!readNewDataEntry()) {
				return null;
			}
		}
		
		ridsLeft--;
		int pageID = buffer.getInt();
		int tupleID = buffer.getInt();
		
		int resetIndex = 0;
		int numAttr = DatabaseCatalog.getInstance().getSchema(tableName).getNumCols();
		int tuplesPerPage = 4088 / (4 * numAttr);
		resetIndex = pageID * tuplesPerPage + tupleID;
		tr.reset(resetIndex);
		
		Tuple t = tr.readNextTuple();
		String name;
		if(alias != null)
			name = alias;
		else
			name = tableName;
		int i = t.getFields().indexOf(name + "." + sortAttr);
		int x = Integer.parseInt(t.getValues().get(i));
		if (x > highKey)
			return null;
		return t;
	}
	
	/**
	 * Adjusts the position of the buffer to the next data entry.
	 * @return True if there are more data entries left.
	 */
	private boolean readNewDataEntry() {
		if (dataEntriesLeft == 0) {
			if(!readNewLeafPage()) {
				return false;
			}
		}
		dataEntriesLeft--;
		int key = buffer.getInt();
		ridsLeft = buffer.getInt();
		return true;
	}

	/**
	 * Reads in the next page and fills the buffer.
	 * @return True if this is a leaf page.
	 */
	private boolean readNewLeafPage() {
		setBufferToAddress(++currentPage);
		int isIndexNode = buffer.getInt();
		dataEntriesLeft = buffer.getInt();
		if(!(isIndexNode == 1)) {
			return true;
		}
		return false;
	}

	/**
	 * Fills the buffer with the page with the given index.
	 * @param addr Index of the page we want.
	 */
	private void setBufferToAddress(int addr) {
		buffer = ByteBuffer.allocate(4096);
		try {
			fc.position(addr * 4096);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			if (fc.read(buffer) < 1) {
				System.out.println("ERR: reached end of FileChannel");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer.position(0);
	}
	
	/**
	 * Resets the IndexReader so the next call to readNextTuple()
	 * returns as it would if it were just instantiated.
	 */
	public void reset() {
		try {
			fc.position(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer = ByteBuffer.allocate(4096);
		
		handleFirstDescent();
	}
}
