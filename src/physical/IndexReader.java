package physical;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

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
	private boolean notInIndex = false;
	
	/**
	 * Constructs an IndexReader.
	 * @param indexDir The directory of the index file.
	 * @param lowKey The low key (inclusive).
	 * @param highKey The high key (inclusive).
	 * @param clustered True if the index is clustered.
	 */
	public IndexReader(String indexDir, int lowKey, int highKey, boolean clustered, String tableName, String alias) {
		this.lowKey = lowKey;
		this.highKey = highKey;
		this.clustered = clustered;
		tr = new TupleReader(tableName, alias, false, null, null); // TODO Laura is this right?
		this.tableName = tableName;
		
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
		while(lowKey > key && dataEntriesLeft > 0) {
			// set position for next key
			key = buffer.getInt();
			int numRids = buffer.getInt();
			int oldPos = buffer.position();
			if(oldPos + (numRids * 2) * 4 > 4095) {
				System.out.println("Not enough room in buffer.");
			}
			buffer.position(oldPos + (numRids * 2) * 4);
			dataEntriesLeft--;
		}
		if (dataEntriesLeft == 0)
			notInIndex = true;
		else {
			// at key that is greater than or equal to lowKey
			ridsLeft = buffer.getInt();
			// the buffer is now positioned to return the first rid
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
		if (notInIndex)
			return null;
		
		if (clustered) {
			return tr.readNextTuple();
		}
		//else.. 
		if (ridsLeft == 0) {
			if(!readNewDataEntry()) {
				return null;
			}
		}
		ridsLeft--;
		int pageID = buffer.getInt();
		int tupleID = buffer.getInt();
		
		int resetIndex = 0;
		int numAttr = DatabaseCatalog.getInstance().getSchema(tableName).getNumCols();
		int tuplesPerPage = 4088 / (4 * numAttr);		//4088 to account 8 bytes metadata
		resetIndex = pageID * tuplesPerPage + tupleID * 4;
		tr.reset(resetIndex);
		return tr.readNextTuple();
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
		currentPage++;
		try {
			fc.position(currentPage * 4096);
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
