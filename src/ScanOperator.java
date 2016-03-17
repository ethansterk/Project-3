/**
 * The ScanOperator is a relational algebra operator that scans a base table.
 * It opens a file scan on the appropriate data file on initialization.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class ScanOperator extends Operator {
	
	private String tablename;
	private Table t;
	private String alias = null;
	private int pointer = 0;		//points to current tuple
	
	/**
	 * Initialize a scan operator with a specific table to scan.
	 * @param s Name of the table.
	 */
	public ScanOperator(String s) {
		tablename = s.split(" ")[0];		//trims off the alias, if there's one
		if (s.split(" ").length > 1) 
			alias = s.split(" ")[2];
		t = new Table(tablename, alias);
	}

	/**
	 * Directly accesses the next tuple from the Table t. Increments
	 * pointer.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple temp = t.getTuple(pointer);
		pointer++;
		return temp;
	}

	@Override
	public void reset() {
		pointer = 0;
	}
}
