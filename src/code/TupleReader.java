package code;
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
	private ArrayList<String> fields = new ArrayList<String>();
	private String name;
	
	/**
	 * Initialize the TupleReader. Retrieves the file channel. Allocates
	 * the buffer. Calculates the number of pages that need to be read from
	 * file. Assigns the tableName as simply fileName.
	 * @param fileName Name of the file containing the table's tuples.
	 */
	public TupleReader(String fileName, String alias, boolean extsort, String filePath, ArrayList<String> fields) {
		if (extsort) {
			this.fields = fields;
			numAtt = this.fields.size();
			
			FileInputStream fin = null;
			try {
				fin = new FileInputStream(filePath);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			fc = fin.getChannel();
			name = filePath.substring(filePath.indexOf("@"));
		}
		else {
			name = fileName;
			Schema sch = DatabaseCatalog.getInstance().getSchema(fileName);
			String tableDir = sch.getTableDir();
			ArrayList<String> tempFields = sch.getCols();
			
			if (alias != null) {
				for (String c : tempFields) {
					this.fields.add(alias + "." + c);
				}
			}
			else {
				for (String c : tempFields)
					this.fields.add(fileName + "." + c);
			}
			numAtt = this.fields.size();
		
			//retrieve the file channel
			FileInputStream fin = null;
			try {
				fin = new FileInputStream(tableDir);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			fc = fin.getChannel();
		}
		
		//calculate number of pages for file
		long numBytes = -1;
		try {
			numBytes = fc.size();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(numBytes % 4096 == 0)
			numPagesLeft = numBytes / 4096;
		else
			numPagesLeft = numBytes / 4096 + 1;
		
		buffer = ByteBuffer.allocate(4096);
	}
	
	/**
	 * Fills the buffer with tuples once all existing tuples have been
	 * read in. If this is the second-to-last page, it also clears the
	 * leftover tuples from the previous page.
	 */
	private void readNewPage() {
		buffer = ByteBuffer.allocate(4096);
		
		try {
			if (fc.read(buffer) < 1) {
				System.out.println("ERR: reached end of FileChannel");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		/*numAtt = */buffer.getInt(0);
		numTuplesLeft = buffer.getInt(4);
		buffer.position(8);
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
		
		if (buffer.remaining() < (4 * numAtt)) return null;
		
		String data = "";
		for (int i = 0; i < numAtt; i++) {
			data += buffer.getInt() + ",";
		}
		//trim off trailing comma "5,6,7,8," -> "5,6,7,8"
		data = data.substring(0, data.length() - 1);
		
		Tuple t = new Tuple(data, fields);
		numTuplesLeft--;
		return t;
	}
	
	/**
	 * Check if we've reached the end of the buffer. 
	 * Returns:
	 * 	-1 = no
	 * 	0  = yes, but there's more pages
	 * 	1  = yes, and there's no more pages
	 */
	public int reachedEnd() {
		return (numTuplesLeft == 0 && numPagesLeft == 0) ? 1 : ((numTuplesLeft > 0) ? -1 : 0);
	}
	
	/**
	 * Getter method for the number of attributes for this table/buffer.
	 * @return numAtt
	 */
	public int getNumAtt() {
		return numAtt;
	}
	
	/**
	 * Used by ExternalSortOperator when SMJOperator is the parent requesting
	 * the sort operator to reset to a certain partition (index of desired tuple).
	 * @param i
	 */
	public void reset(int i) {		
		//calculate number of pages for file
		long numBytes = -1;
		try {
			numBytes = fc.size();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(numBytes % 4096 == 0)
			numPagesLeft = numBytes / 4096;
		else
			numPagesLeft = numBytes / 4096 + 1;
		
		//compute number of page that tuple will be on, first page = 0
		//and index of tuple on that page, starting with 0
		int pageNum = -1;
		int tupleIndexOnPage = -1;
		int tuplesPerPage = 4088 / (4 * numAtt);		//4088 to account 8 bytes metadata
		pageNum = i / tuplesPerPage;
		tupleIndexOnPage = i % tuplesPerPage;
		
		//update numPagesLeft and numTuplesLeft
		numPagesLeft = numPagesLeft - pageNum;
		numTuplesLeft = 0;
		
		//fetch that page - can reset position of FileChannel to start of desired page within file using position(long newPosition)
		//and update numPagesLeft
		try {
			fc.position(pageNum * 4096);
		} catch (IOException e) {
			e.printStackTrace();
		}
		readNewPage();
		
		if (tupleIndexOnPage != 0) {
			try {
				fc.position(fc.position() + 8 + (tupleIndexOnPage * 4 * numAtt));
			} catch (IOException e) {
				e.printStackTrace();
			}
			numTuplesLeft -= tupleIndexOnPage;
		}
	}
}
