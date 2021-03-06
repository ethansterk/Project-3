package code;

import java.util.ArrayList;

/**
 * 
 * The Stats class is used for storing information about a particular relation.
 * This is used for both generating statistics and using this information in
 * the physical and logical query plans.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class Stats {

	private String relName;
	private int numTuples;
	private ArrayList<ColStats> cols;
	
	/**
	 * Initializes the stats for this particular schema.
	 * @param relName
	 * @param numTuples
	 * @param colStats
	 */
	public Stats(String relName, int numTuples, ArrayList<ColStats> colStats) {
		this.relName = relName;
		this.numTuples = numTuples;
		this.cols = colStats;
	}

	public String getRelName() {
		return relName;
	}

	public void setRelName(String relName) {
		this.relName = relName;
	}

	public int getNumTuples() {
		return numTuples;
	}

	public void setNumTuples(int numTuples) {
		this.numTuples = numTuples;
	}

	public ArrayList<ColStats> getCols() {
		return cols;
	}
	
	public ColStats getColWithName(String colName) {
		for (ColStats col : cols) {
			if (col.getColName().equals(colName))
				return col;
		}
		return null;
	}

	public void setCols(ArrayList<ColStats> cols) {
		this.cols = cols;
	}
}
