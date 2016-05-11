package code;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import physical.ScanOperator;

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
	private Stats stats;

	/**
	 * Initializes a schema with given name and columns.
	 * 
	 * @param s String that contains the name of the relation and names of the columns,
	 * separated by spaces.
	 * @param inputDir String that contains the directory of the /input/ file.
	 */
	public Schema(String s, String inputDir, File stats) {
		String[] tokens = s.split(" ");
		name = tokens[0];
		columns = new ArrayList<String>(Arrays.asList(tokens));
		columns.remove(0);
		numcols = columns.size();
		tableDir = inputDir + File.separator + "db" + File.separator + "data" + File.separator + name;
		generateStats(stats, name);
	}
	
	/**
	 * Another constructor was necessary to initialize Schemas before
	 * putting them into the HashMap in DBCatalog. The reason we needed
	 * this catalog is because when we initialize the Schema again, we
	 * rely on a TupleReader to scan the file, and the TupleReader relies
	 * on this HashMap existing.
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
	}
	
	/**
	 * Gathers statistics on the relation relevant to this schema and
	 * writes these to the statistics text file.
	 * @param stats File than contains statistics.
	 * @param tablename Full name of the relations (i.e. Sailors, Boats, etc.).
	 */
	private void generateStats(File stats, String tablename) {
		String relName = name;
		int numTuples = 0;
		ArrayList<ColStats> relColStats = new ArrayList<ColStats>();
		for(String colName : columns) {
			ColStats temp = new ColStats(colName);
			relColStats.add(temp);
		}

		ScanOperator statsScan = new ScanOperator(tablename);
		Tuple t;
		while((t = statsScan.getNextTuple()) != null) {
			numTuples++;
			for (ColStats c : relColStats) {
				String col = c.getColName();
				int i = t.getFields().indexOf(tablename + "." + col);
				int val = Integer.valueOf(t.getValues().get(i));
				int max = c.getMaxVal();
				int min = c.getMinVal();
				if (val > max)
					c.setMaxVal(val);
				if (val < min)
					c.setMinVal(val);
			}
		}

		String statsMessage = "";
		statsMessage += relName + " " + numTuples;
		for (ColStats c : relColStats) {
			statsMessage += " " + c.getColName() + "," + c.getMinVal() + "," + c.getMaxVal();
		}
		
		// Create a Stats object for this relation
		this.stats = new Stats(relName, numTuples, relColStats);
		
		// Write to statistics file
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(new FileWriter(stats, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
		writer.println(statsMessage);
		writer.close();
	}
	
	/**
	 * Returns the Stats object for this relation.
	 * 
	 * @return stats Stats object for this relation.
	 */
	public Stats getStats() {
		if (stats == null)
			System.out.println("Stats is null for " + name);
		return stats;
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
