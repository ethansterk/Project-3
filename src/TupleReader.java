import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * The TupleReader class encapsulates the log for getting tuple from
 * a page. It reads one page at a time by using a ByteBuffer, which
 * it fills using read calls to an appropriate FileChannel. Then,
 * it extracts specific tuples from the page as needed when someone
 * requests the next tuple. 
 * 
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class TupleReader {

	FileChannel fc;
	ByteBuffer buffer;
	long numPagesLeft;
	int numAtt; //number of attributes
	int numTuplesLeft; //number of tuples on page
	String tableName;
	
	public TupleReader(String fileName) {
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fin.getChannel();
		buffer = ByteBuffer.allocate( 4096 );
		
		//calculate number of pages for file
		long numBytes = -1;
		try {
			numBytes = fc.size();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(numBytes % 4096 == 0) {
			numPagesLeft = numBytes / 4096;
		} else {
			numPagesLeft = numBytes / 4096 + 1;
		}
		
		tableName = fileName;
	}
	
	/**
	 * Reads the next tuple from the page using ByteBuffer.
	 * @return the next Tuple
	 */
	public Tuple readNextTuple() {
		if (numTuplesLeft == 0 && numPagesLeft > 0) {
			readNewPage();
		}
		else if (numTuplesLeft == 0 && numPagesLeft == 0) {
			return null;
		}
		
		String data = "";
		for (int i = 0; i < numAtt; i++) {
			data += buffer.getInt() + ",";
		}
		data = data.substring(0, data.length() - 1);
		
		ArrayList<String> fields = DatabaseCatalog.getInstance().getSchema(tableName).getCols();
		
		Tuple t = new Tuple(data, fields);
		return t;
	}
	
	/**
	 * Fills the buffer with tuples once all existing tuples have been
	 * read in. If this is the second-to-last page, it also clears the
	 * leftover tuples from the previous page.
	 */
	private void readNewPage() {
		if (numPagesLeft == 1) {
			System.out.println("CLEAR THE BUFFER");
		}
		
		try {
			if (fc.read(buffer) < 1) {
				Logger.log("ERR: reached end of FileChannel");
				//reached end of FileChannel
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		//assign numAtt, numTuples
		numAtt = buffer.getInt();
		numTuplesLeft = buffer.getInt();
		numPagesLeft--;
	}
	
	/**
	 * 
	 */
	public void close() {
		
	}
	
	/**
	 * 
	 */
	public void reset() {
		
	}
	
}
