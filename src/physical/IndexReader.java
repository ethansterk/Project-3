package physical;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexReader {

	private FileChannel fc;
	private ByteBuffer buffer;
	private int ROOT_ADDR;
	private int NUM_LEAVES;
	private int ORDER;
	
	public IndexReader(String indexDir) {
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
			int pageAddr = 0; // TODO set to the next page address
			// go to new page
			setBufferToAddress(pageAddr);
		}
	}

	private void readNewPage() {
		buffer = ByteBuffer.allocate(4096);
		
		try {
			if (fc.read(buffer) < 1) {
				System.out.println("ERR: reached end of FileChannel");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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
