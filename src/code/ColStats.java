package code;

/**
 * The ColStats class is used to help in the generation of statistics for
 * each relation. For every column of the relation, a ColStats object is
 * created. When scanning the relation, the fields of the ColStats object
 * (min and max value) are updated appropriately.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class ColStats {
	private String colName;
	private int minVal;
	private int maxVal;
	
	/**
	 * Initializes the ColStats object. The rest of this class is straightforward.
	 * @param colName Name of the column.
	 */
	public ColStats(String colName) {
		this.colName = colName;
		minVal = Integer.MAX_VALUE;
		maxVal = Integer.MIN_VALUE;
	}

	public int getMinVal() {
		return minVal;
	}

	public int getMaxVal() {
		return maxVal;
	}

	public void setMinVal(int minVal) {
		this.minVal = minVal;
	}

	public void setMaxVal(int maxVal) {
		this.maxVal = maxVal;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}
}
