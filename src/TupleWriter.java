import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * TupleWriter contains a method to write a tuple to file.
 * It buffers tuples in memory until it has a full page then 
 * flushes (writes) that page.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class TupleWriter {

	private FileChannel fc;
	private ByteBuffer buffer;
	private int numAtt;
	private int numTuples;
	
	/**
	 * Constructs a TupleWriter with a given file name. Allocates
	 * the buffer.
	 * @param fileName Name of file containing table.
	 */
	public TupleWriter(String fileName) {
		FileOutputStream fout = null;
		try {
			fout = new FileOutputStream(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fout.getChannel();
		
		buffer = ByteBuffer.allocate(4096);
	}
	
	/**
	 * Writes out the current buffer to the output stream. Reallocates 
	 * the buffer to start accepting more tuples.
	 * Also, overwrites the metadata for the number of tuples now stored
	 * in the current page being written out.
	 */
	public void writeNewPage() {
		//overwrite metadata
		buffer.putInt(4, numTuples);
		
		//flip buffer
		buffer.flip();
		//output buffer to channel
		try {
			fc.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//clear buffer
		buffer = ByteBuffer.allocate(4096);
		//TODO add metadata to new buffer --here or in writeTuple()?
		
	}
	
	/**
	 * Adds tuple to the buffer, which will write to file
	 * when full.
	 * @param t Tuple to be written to file
	 */
	public void writeTuple(Tuple t) {
		//t null implies last tuple to write
		if (t == null) {
			writeNewPage();
		}
		
		//break tuple into manageable chunks
		ArrayList<String> data = t.getValues();
		
		if (buffer.limit() == 0) { //first write to page
			numAtt = data.size();
			numTuples = 0; //temporary placeholder
			buffer.putInt(numAtt);
			buffer.putInt(numTuples);
		}
		
		//check if buffer is full
		int tupleByteLength = 4 * data.size();
		if(buffer.capacity() - buffer.limit() < tupleByteLength) {
			writeNewPage();
		}
		
		//add tuple to buffer, chunk by chunk
		for(int i = 0; i < data.size(); i++) {
			//put() data into buffer, checking if buffer is full
			int data_i = Integer.valueOf(data.get(i));
			buffer.putInt(data_i);
		}
		numTuples++;
	}
	
	
}
