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
	
	public TupleWriter(String fileName) {
		FileOutputStream fout = null;
		try {
			fout = new FileOutputStream(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fc = fout.getChannel();
	}
	
	public void writeNewPage() {
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
		//break tuple into manageable chunks
		ArrayList<String> data = t.getValues();
		
		//check if buffer is full
		int tupleLength = 4 * data.size();
		if(buffer.capacity() - buffer.limit() < tupleLength) {
			writeNewPage();
		}
		
		//add tuple to buffer, chunk by chunk
		for(int i = 0; i < data.size(); i++) {
			//put() data into buffer, checking if buffer is full
			int data_i = Integer.valueOf(data.get(i));
			buffer.putInt(data_i);
		}
	}
	
	
}
