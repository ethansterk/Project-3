package code;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The Schema class keeps track of each table's information such as:
 * 	- name
 *  - column labels
 *  - number of columns
 *  - directory that points to the actual table file
 * Each Schema is instantiated at the start of a program when DatabaseCatalog is running.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class Schema {

	private String name;
	private ArrayList<String> columns;
	private int numcols;
	private String tableDir;
	
	/**
	 * Initializes a schema with given name and columns.
	 * 
	 * @param s String that contains the name of the relation and names of the columns,
	 * separated by spaces.
	 * @param inputDir String that contains the directory of the /input/ file.
	 */
	public Schema(String s, String inputDir) {
		String[] tokens = s.split(" ");
		name = tokens[0];
		columns = new ArrayList<String>(Arrays.asList(tokens));
		columns.remove(0);
		numcols = columns.size();
		tableDir = inputDir + File.separator + "db" + File.separator + "data" + File.separator + name;
		generateStats();
	}
	
	private void generateStats() {
		// TODO generate stats about relations
		// RelationName NumTuples ColName,Min,Max
		// RelationName - can get from schemas (loop?)
		// NumTuples - have to scan each relation
		// ColName,Min,Max - maintain info as scanning relation (all available in Tuple)
		String relName = name;
		int numTuples = 0;
		ArrayList<ColStats> relColStats = new ArrayList<ColStats>();
		for(String colName : columns) {
			ColStats temp = new ColStats(colName);
			relColStats.add(temp);
		}
		// scan relation
			// log info
		// format to match formatting above
		// output to stats file (having had created it in DatabaseCatalog)
	}

	/**
	 * Returns the name of this relation.
	 * 
	 * @return name Name of relation.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Returns the names of the columns.
	 * 
	 * @return columns Names of columns.
	 */
	public ArrayList<String> getCols() {
		return columns;
	}
	
	/**
	 * Returns the total number of columns of this relation.
	 * 
	 * @return numcols Number of columns.
	 */
	public int getNumCols() {
		return numcols;
	}
	
	/**
	 * Returns the directory that points to the actual table for this relation.
	 * 
	 * @return tableDir Directory to table.
	 */
	public String getTableDir() {
		return tableDir;
	}
}
