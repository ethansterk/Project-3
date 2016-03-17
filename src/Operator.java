import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

/**
 * The Operator class is an abstract representation of the relational algebra
 * operators that extend it. It, like the relational algebra equivalent,
 * takes a relation instance(s) as input and returns an instance.
 * To access the input relation instance(s), it calls repeatedly
 * the getNextTuple() function, which returns one tuple from the
 * input (see Tuple class).
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 * 
 */
public abstract class Operator {
	
	/**
	 * Get the next tuple of the Operator's output.
	 * 
	 * @return The next tuple or null if there is no output available.
	 */
	public abstract Tuple getNextTuple();
	
	/**
	 * Tells the Operator to reset its state and start returning its output
	 * again from the beginning. A subsequent call to getNextTuple() will 
	 * return the first tuple in that Operator's output.
	 */
	public abstract void reset();
	
	/**
	 * Repeatedly calls getNextTuple() until next tuple is 
	 * null (no more output) and writes each tuple to a suitable PrintStream.
	 */
	public void dump() {
		int queryNumber = OutputWriter.getInstance().getQueryNumber();
		String outputDir = OutputWriter.getInstance().getOutputDir();
		
		String filename = outputDir + File.separator + "query" + queryNumber;
		File f = new File(filename);
		
		try {
			
			PrintStream output = new PrintStream(f);
			Tuple t = getNextTuple();
			while (t != null) {
				//write Tuple t to a PrintStream
				//System.out.println(t.tupleString());
				output.println(t.tupleString());
				t = getNextTuple();
			}
			output.flush();
			output.close();
			
		} catch (FileNotFoundException e) {
			System.out.println("Error in finding file for dumping");
			e.printStackTrace();
		}
	}
}
