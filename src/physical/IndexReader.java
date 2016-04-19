package physical;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import code.Tuple;
import code.TupleReader;

public class IndexReader {

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
	
	public IndexReader(String indexDir, int lowKey, int highKey, boolean clustered) {
		this.lowKey = lowKey;
		this.highKey = highKey;
		this.clustered = clustered;
		tr = new TupleReader(String fileName, String alias, boolean extsort, String filePath, ArrayList<String> fields); // TODO get all this stuff
		//retrieve the file channel
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(indexDir);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fin.getChannel();
		
		buffer = ByteBuffer.allocate(4096);
		
		handleFirstDecent();
		
	}
	
	private void handleFirstDecent() {
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
		int isIndexNode = 0;
		while((isIndexNode = buffer.getInt()) == 1) {
			int nextPageAddr = -1; // TODO set to the next page address
			int numTuples = buffer.getInt();
			for(int i = 0; i < numTuples; i++) {
				int key = buffer.getInt();
				if(lowKey < key) {
					int oldPos = buffer.position();
					buffer.position(oldPos + (numTuples - 1) * 4);
					nextPageAddr = buffer.getInt();
				}
			}
			if(nextPageAddr == -1) { // lowKey greater than all keys (addr never assigned)
				int oldPos = buffer.position();
				buffer.position(oldPos + (numTuples) * 4);
				nextPageAddr = buffer.getInt();
			}
			// go to new page
			setBufferToAddress(nextPageAddr);
			currentPage = nextPageAddr;
		}
		// at leaf node, find the key that is greater than or equal to lowKey
		dataEntriesLeft = buffer.getInt();
		int key;
		while(lowKey < (key = buffer.getInt())) {
			// set position for next key
			int numRids = buffer.getInt();
			int oldPos = buffer.position();
			buffer.position(oldPos + (numRids * 2) * 4); // TODO might go out-of-bounds if key does not exist/ no tuples to be returned
			dataEntriesLeft--;
		}
		// at key that is greater than or equal to lowKey
		ridsLeft = buffer.getInt();
		// the buffer is now positioned to return the first rid
		// TODO set up tuple reader here?
		// TODO handle case where lowKey is null (returns first leafnode)
	}

	public Tuple readNextTuple() {
		if (clustered) {
			return tr.readNextTuple();
		}
		
		
		// read next rid (check if any left)
		// use rid to retrieve tuple from TupleReader
		// if clustered, just call getNextTuple of TupleReader
		// if unclustered, use reset to reset tuple reader
		if (ridsLeft == 0) {
			if(!readNewDataEntry()) {
				return null;
			}
		}
		ridsLeft--;
		int pageID = buffer.getInt();
		int tupleID = buffer.getInt();
		
		int resetIndex = 0;
		// TODO get numAtt from size of fields (passed into IndexReader)
		int tuplesPerPage = 4088 / (4 * numAtt);		//4088 to account 8 bytes metadata
		resetIndex = pageID * tuplesPerPage + tupleID * 4;
		tr.reset(resetIndex);
		return tr.readNextTuple();
	}
	
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

	private void setBufferToAddress(int addr) {
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
}
