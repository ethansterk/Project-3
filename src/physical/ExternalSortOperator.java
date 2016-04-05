package physical;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import code.OrderComparator;
import code.OutputWriter;
import code.Tuple;
import code.TupleReader;
import code.TupleWriter;
import net.sf.jsqlparser.statement.select.OrderByElement;

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

	private boolean binaryIO = true;
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
	OrderComparator oc;
	TupleReader tr;
	
	public ExternalSortOperator(Operator child, int numBufferPages, List<OrderByElement> list) {
		System.out.println("Made an ExternalSortOperator");
		this.child = child;
		this.B = numBufferPages;
		if (list == null)
			list = null;
		else {
			for (OrderByElement x : list) {
				columns.add(x.getExpression().toString());
			}
		}
		//standardize the temp directory we're writing to
		id = child.toString();
		tempDir = OutputWriter.getInstance().getTempDir();
		
		//calculate size of buffer
		fields = this.child.getNextTuple().getFields();
		int numAttr = fields.size();
		this.child.reset();	//clear left again
		numTuples = B * 4096 / (4 * numAttr);
		
		oc = new OrderComparator(columns, fields);
		
		doPassZero();
		
		doMergePass();
		while (runs > 1) {
			doMergePass();
		}
		tr = new TupleReader(null, null, true, finalFileLocation, fields);
	}
	
	/**
	 * Helper method used to execute Pass 0. Here, we sort B pages
	 * in-memory and then write the buffer to external memory (a 
	 * serialized file representing a run).
	 */
	private void doPassZero() {
		int fileNum = 0;

		readBPages();
		
		while (buffer.get(0) != null) {
			Collections.sort(buffer, oc);
			
			String filename = tempDir + File.separator + id + "-p0-" + fileNum;
			File f = new File(filename);		//don't delete!
			forcePrint(numTuples, filename);
			
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
		int readingFileNum = 0;
		int writingFileNum = 0;
		int thisPass = passes + 1;
		
		//check for corner case where there's only one run after Pass 0
		if (runs <= 1) {
			finalFileLocation = tempDir + File.separator + id + "-p0-0";
			return;
		}
		
		while (readingFileNum < runs) {
			//create the buffer pool
			ArrayList<TupleReader> bufferPool = new ArrayList<TupleReader>();
			for (int i = 0; i < B - 1; i++) {
				//prevent an error from trying to read from a file that doesn't exist
				if (readingFileNum >= runs)
					continue;
				else {
					String filePath = tempDir + File.separator + id + "-p" + passes + "-" + readingFileNum;
					bufferPool.add(new TupleReader(null, null, true, filePath, fields));
					bufferPool.get(i).readNewPage();
				}
				readingFileNum++;
			}
			
			//create the remaining buffer page for output
			buffer = new ArrayList<Tuple>();

			//grab the first Tuples to be compared and merged
			Tuple[] topTuples = new Tuple[B - 1];
			for (int i = 0; i < B - 1; i++) {
				topTuples[i] = bufferPool.get(i).readNextTuple();
			}
			
			String filename = tempDir + File.separator + id + "-p" + thisPass + "-" + writingFileNum;
			File f = new File(filename);		//don't delete!
			int t = 0;
			while (bufferPool.size() > 0) {
				//compare the tuples at the top of each buffer
				for (int i = 0; i < topTuples.length; i++) {
					int comp = oc.compare(topTuples[t], topTuples[i]);
					if (comp > 0) 
						t = i;
				}
				
				//write the next tuple to the remaining buffer
				writeToBuffer(topTuples[t], filename);
				
				//refresh the buffer pool
				if (bufferPool.get(t).reachedEnd() != 1) {
					topTuples[t] = bufferPool.get(t).readNextTuple();
				}
				else {
					bufferPool.remove(t);
					Tuple[] tempTuples = new Tuple[topTuples.length - 1];
					int j = 0;
					for (int i = 0; i < topTuples.length; i++) {
						if (i != t) {
							tempTuples[j] = topTuples[i];
							j++;
						}
					}
				}

				t = 0;
			}
			forcePrint(buffer.size(), filename);   //make sure last page is written
			
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
			buffer.add(child.getNextTuple());
			//System.out.println(buffer.get(i).tupleString());
			if(buffer.get(i) == null) {
				return;
			}
		}
		return;
	}
	
	/**
	 * Writes a tuple to the output buffer during a merge pass.
	 * Forces the output buffer to disk one page at a time. 
	 */
	private void writeToBuffer(Tuple t, String fileLoc) {
		int tuplesPerPage = numTuples / B;		//will be a whole number b/c of how we compute numTuples
		buffer.add(t);
		
		//force a page to print
		if (buffer.size() == tuplesPerPage) {
			forcePrint(tuplesPerPage, fileLoc);
		}
	}
	
	/**
	 * For pass 0, 
	 * For merge pass, forces the output buffer to disk one page at a time.
	 */
	private void forcePrint(int numTuples, String fileLoc) {
		if (binaryIO) {
			TupleWriter tw = new TupleWriter(fileLoc);

			for (int i = 0; i < numTuples; i++) {
				Tuple tup = buffer.get(i);
				
				//if (last) buffer isn't completely full, stop writing early
				if (tup == null)
					break;
				else
					tw.writeTuple(tup);
			}
			tw.writeNewPage();
		}
		else {
			try {
				PrintStream output = new PrintStream(fileLoc);
				
				for (int i = 0; i < numTuples; i++) {
					Tuple tup = buffer.get(i);
					
					//if (last) buffer isn't completely full, stop writing early
					if (tup == null)
						break;
					else
						output.println(tup.tupleString());
				}
				output.flush();
				output.close();
				
			} catch (FileNotFoundException e) {
				System.out.println("Error in finding file for writing tuples for external sort.");
				e.printStackTrace();
			}
		}
	}

	@Override
	public Tuple getNextTuple() {
		return tr.readNextTuple();
	}

	@Override
	public void reset() {
		tr = new TupleReader(null, null, true, finalFileLocation, fields);
		
	}
}
