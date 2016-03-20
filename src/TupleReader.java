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

	private FileChannel fc;
	private ByteBuffer buffer;
	private long numPagesLeft;
	private int numAtt; //number of attributes
	private int numTuplesLeft; //number of tuples on page
	private String tableName;
	
	/**
	 * Initialize the TupleReader. Retrieves the file channel. Allocates
	 * the buffer. Calculates the number of pages that need to be read from
	 * file. Assigns the tableName as simply fileName.
	 * @param fileName Name of the file containing the table's tuples.
	 */
	public TupleReader(String fileName) {
		//retrieve the file channel
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fin.getChannel();
		
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
		
		buffer = ByteBuffer.allocate( 4096 );
		tableName = fileName;
	}
	
	/**
	 * Fills the buffer with tuples once all existing tuples have been
	 * read in. If this is the second-to-last page, it also clears the
	 * leftover tuples from the previous page.
	 */
	private void readNewPage() {
		if (numPagesLeft == 1) {
			//see documentation -- assigns all to zero
			buffer = ByteBuffer.allocate(4096);
		}
		
		try {
			if (fc.read(buffer) < 1) {
				System.out.println("ERR: reached end of FileChannel");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		numAtt = buffer.getInt();
		numTuplesLeft = buffer.getInt();
		numPagesLeft--;
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
		//trim off trailing comma "5,6,7,8," -> "5,6,7,8"
		data = data.substring(0, data.length() - 1);
		
		ArrayList<String> fields = DatabaseCatalog.getInstance().getSchema(tableName).getCols();
		
		Tuple t = new Tuple(data, fields);
		return t;
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
