package physical;

import code.Table;
import code.Tuple;
import code.TupleReader;

/**
 * 
 * The IndexScan class represents a variation on the traditional
 * Scan operator. It is intended to be more efficient in that it
 * utilizes a B+-tree to retrieve tuples from a given relation.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class IndexScan extends Operator{

	private String tablename;
	private String alias = null;
	private IndexReader ir;
	
	/**
	 * This constructs an IndexScan and initializes the first data entry
	 * for retrieving tuples in the future.
	 * @param indexDir Directory of the index.
	 * @param s Relation/alias name
	 * @param clustered True is the index is clustered
	 * @param lowKey Lower bound (inclusive) on the condition
	 * @param highKey Upper bound (inclusive) on the condition
	 */
	public IndexScan(String indexDir, String s, boolean clustered, int lowKey, int highKey) {
		tablename = s.split(" ")[0];		//trims off the alias, if there's one
		if (s.split(" ").length > 1) 
			alias = s.split(" ")[2];
		
		// create an IndexReader (similar to the TupleReader)
		ir = new IndexReader(indexDir, lowKey, highKey, clustered, tablename, alias);
	}
	
	

	/**
	 * Retrieves the next tuple from the index file. The process
	 * differs if the index file is clustered versus unclustered.
	 */
	@Override
	public Tuple getNextTuple() {
		return ir.readNextTuple();
	}

	/**
	 * Resets the IndexScan operator so that the next call to 
	 * getNextTuple() will return the first one that is returned
	 * at the creation of the operator.
	 */
	@Override
	public void reset() {
		// TODO more?
		ir.reset();
	}

}
