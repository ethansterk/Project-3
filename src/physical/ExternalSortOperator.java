package physical;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

import code.OrderComparator;
import code.OutputWriter;
import code.Tuple;
import code.TupleReader;
import code.TupleWriter;

/**
 * ExternalSortOperator is an alternative to the naive sort method.
 * In the 0th pass, B pages are read in and each sorted. Given N pages in the
 * file to be sorted, we will have N/B runs of B pages each (except maybe the
 * last one). For each subsequent "merge" pass, use (B - 1) buffer pages to
 * hold input and 1 buffer for output--so each merge pass is (B - 1)-way.
 * This sorting ends when there is just one run at the end of the merging.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class ExternalSortOperator extends Operator {

	private boolean binaryIO = false;
	private Operator child;
	private ArrayList<String> columns = new ArrayList<String>();
	private ArrayList<String> fields = new ArrayList<String>();
	private ArrayList<Tuple> buffer = new ArrayList<Tuple>();
	String id = "";
	String tempDir = "";
	private int B = 0;
	private int numTuples = 0;
	private int passes = 0;
	private int runs = 0;
	private String finalFileLocation = "";
	private OrderComparator oc;
	private TupleReader tr;
	private TupleWriter tw;
	private Scanner sc;
	private PrintStream p;
	
	public ExternalSortOperator(Operator child, int numBufferPages, ArrayList<String> list) {
		this.child = child;
		this.B = numBufferPages;
		if (list == null)
			list = null;
		else
			columns.addAll(list); // must use addAll() because passes by reference
		
		//standardize the temp directory we're writing to
		id = child.toString();
		tempDir = OutputWriter.getInstance().getTempDir();
		
		//calculate size of buffer
		fields = this.child.getNextTuple().getFields();
		int numAttr = fields.size();
		this.child.reset();	//clear the child again
		numTuples = B * 4096 / (4 * numAttr);
		
		oc = new OrderComparator(columns, fields);
		
		doPassZero();
		
		doMergePass();
		while (runs > 1)
			doMergePass();
		
		if (binaryIO)
			tr = new TupleReader(null, null, true, finalFileLocation, fields);
		else {
			try {
				sc = new Scanner(new File(finalFileLocation));
				convertToBinary();
				tr = new TupleReader(null, null, true, finalFileLocation, fields);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Helper method used to execute Pass 0. Here, we sort B pages
	 * in-memory and then write the buffer to external memory (a 
	 * serialized file representing a run).
	 */
	private void doPassZero() {
		int fileNum = 0;

		readBPages();
		
		while (buffer.size() > 0) {
			Collections.sort(buffer, oc);
			
			String filename = tempDir + File.separator + id + "-p0-" + fileNum;
			File f = new File(filename);		//don't delete!
			if (binaryIO) {
				tw = new TupleWriter(filename);
				for (Tuple t : buffer) 
					if (t != null) 
						tw.writeTuple(t);
			}
			else {
				try {
					p = new PrintStream(filename);
					for (Tuple t : buffer)
						if (t != null)
							p.println(t.tupleString());
					p.flush();
					p.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}

			readBPages();
			fileNum++;			
		}
		this.runs = fileNum;
	}
	
	/**
	 * Helper method used to execute Pass 1 and beyond. Uses (B - 1) buffer pages
	 * for input and 1 page (via TupleWriter) for output, resulting in a (B - 1)-way merge each pass.
	 */
	private void doMergePass() {
		buffer = new ArrayList<Tuple>();		//clear buffer for use as output buffer in merge passes
		int readingFileNum = 0;
		int writingFileNum = 0;
		int thisPass = passes + 1;
		
		//check for corner case where there's only one run after Pass 0
		if (runs <= 1) {
			finalFileLocation = tempDir + File.separator + id + "-p0-0";
			return;
		}
		
		while (readingFileNum < runs) {
			String filename = tempDir + File.separator + id + "-p" + thisPass + "-" + writingFileNum;
			File f = new File(filename);		//don't delete!
			
			if (binaryIO) {
				//create the buffer pool
				ArrayList<TupleReader> bufferPool = new ArrayList<TupleReader>();
				ArrayList<Tuple> bufferTuples = new ArrayList<Tuple>();
				for (int i = 0; i < B - 1; i++) {
					if (readingFileNum < runs) {
						String filePath = tempDir + File.separator + id + "-p" + passes + "-" + readingFileNum;
						bufferPool.add(new TupleReader(null, null, true, filePath, fields));
						if (bufferPool.get(i).reachedEnd() != 1) {
							Tuple temp = bufferPool.get(i).readNextTuple();
							if (temp != null) 
								bufferTuples.add(temp);
							else {
								bufferPool.remove(i);
								i--;
							}
						}
						else {
							bufferPool.remove(i);
							i--;
						}
						readingFileNum++;
					}
				}
				
				tw = new TupleWriter(filename);
				
				int t = 0;
				while (bufferPool.size() > 0 && bufferTuples.size() > 0) {
					//compare the tuples at the top of each buffer
					for (int i = 0; i < bufferTuples.size(); i++) {
						if (bufferTuples.get(i) == null)
							continue;
						int comp = oc.compare(bufferTuples.get(t), bufferTuples.get(i));
						if (comp > 0) {
							t = i;
						}
					}
					
					//terminate if can't pull anymore tuples
					if (bufferTuples.get(t) == null)
						break;

					//write the next tuple to the output buffer
					tw.writeTuple(bufferTuples.get(t));
					
					//refresh the buffer pool
					if (bufferPool.get(t).reachedEnd() != 1) {
						bufferTuples.remove(t);
						bufferTuples.add(t, bufferPool.get(t).readNextTuple());
					}
					else {
						bufferPool.remove(t);
						bufferTuples.remove(t);
					}

					t = 0;
				}
				//make sure last page is written 
				tw.writeNewPage();
			}
			//human-readable version of above code
			else {
				//create the buffer pool
				ArrayList<Scanner> bufferPool = new ArrayList<Scanner>();
				ArrayList<Tuple> bufferTuples = new ArrayList<Tuple>();
				for (int i = 0; i < B - 1; i++) {
					if (readingFileNum < runs) {
						String filePath = tempDir + File.separator + id + "-p" + passes + "-" + readingFileNum;
						try {
							bufferPool.add(new Scanner(new File(filePath)));
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						}
						if (bufferPool.get(i).hasNextLine()) {
							String s = bufferPool.get(i).nextLine();
							if (s != null)
								bufferTuples.add(new Tuple(s, fields));
							else {
								bufferPool.remove(i);
								i--;
							}
						}
						else {
							bufferPool.remove(i);
							i--;
						}
						readingFileNum++;
					}
				}
				
				try {
					p = new PrintStream(filename);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				
				int t = 0;
				while (bufferPool.size() > 0 && bufferTuples.size() > 0) {
					//compare the tuples at the top of each buffer
					for (int i = 0; i < bufferTuples.size(); i++) {
						if (bufferTuples.get(i) == null)
							continue;
						int comp = oc.compare(bufferTuples.get(t), bufferTuples.get(i));
						if (comp > 0) {
							t = i;
						}
					}
					
					//terminate if can't pull anymore tuples
					if (bufferTuples.get(t) == null)
						break;

					//write the next tuple to the output buffer
					p.println(bufferTuples.get(t).tupleString());
					
					//refresh the buffer pool
					if (bufferPool.get(t).hasNextLine()) {
						bufferTuples.remove(t);
						bufferTuples.add(t, new Tuple(bufferPool.get(t).nextLine(), fields));
					}
					else {
						bufferPool.remove(t);
						bufferTuples.remove(t);
					}

					t = 0;
				}
				p.flush();
				p.close();
			}
			
			//repeat with new buffer pool for next run
			writingFileNum++;
		}
		
		//update passes and runs
		passes = thisPass;
		runs = writingFileNum;
		
		//check for termination condition: only 1 run left
		if (runs <= 1)
			finalFileLocation = tempDir + File.separator + id + "-p" + passes + "-" + 0;
	}
	
	/**
	 * Reads tuples in from the child, filling the buffer with B pages.
	 * We also sort it in-memory here.
	 */
	private void readBPages() {
		buffer = new ArrayList<Tuple>();
		for(int i = 0; i < numTuples; i++) {
			Tuple temp = child.getNextTuple();
			if (temp == null)
				return;
			buffer.add(temp);
		}
		return;
	}

	@Override
	public Tuple getNextTuple() {
		return tr.readNextTuple();
	}
	
	/**
	 * When using human-readable mode, it guarantees that the final run is at least in binary.
	 * This is so that we can access the final file using a TupleReader in all cases, especially
	 * for the SMJOperator's partitioning algorithm.
	 */
	private void convertToBinary() {
		String finalLoc = tempDir + File.separator + id + "-p" + passes + "-0binary";
		File f = new File(finalLoc);		//don't delete!

		TupleWriter convert = new TupleWriter(finalLoc);
		while (sc.hasNextLine()) {
			String s = sc.nextLine();
			if (s != null) convert.writeTuple(new Tuple(s, fields));
		}
		convert.writeNewPage();
		
		finalFileLocation = finalLoc;
	}

	@Override
	public void reset() {
		tr = new TupleReader(null, null, true, finalFileLocation, fields);
		try {
			sc = new Scanner(new File(finalFileLocation));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void reset(int i) {
		tr.reset(i);
	}
}