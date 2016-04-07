package physical;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import code.OutputWriter;
import code.Tuple;
import code.TupleWriter;

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
	
	private boolean project_three_io = true;
	
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
	 * Used only in the sort operators to reset to a particular index in 
	 * the relation.
	 * @param i Index in relation to reset to.
	 */
	public void reset(int i) {
		System.out.println("Should not be called from Operator class.");
	}
	
	/**
	 * Repeatedly calls getNextTuple() until next tuple is 
	 * null (no more output) and writes each tuple to a suitable PrintStream.
	 */
	public void dump() {
		reset();
		int queryNumber = OutputWriter.getInstance().getQueryNumber();
		String outputDir = OutputWriter.getInstance().getOutputDir();
		
		String filename = outputDir + File.separator + "query" + queryNumber;
		File f = new File(filename);
		
		if (project_three_io) {
			TupleWriter tw = new TupleWriter(filename);
			Tuple t = getNextTuple();
			
			//in the special case that there are no matching tuples whatsoever
			if (t == null) {
				return;
			}
			
			while (t != null) {
				tw.writeTuple(t);
				t = getNextTuple();
			}
			tw.writeNewPage();
		}
		else {
			try {
				PrintStream output = new PrintStream(f);
				Tuple t = getNextTuple();
				while (t != null && t.getFields() != null) {
					//write Tuple t to a PrintStream
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
}
