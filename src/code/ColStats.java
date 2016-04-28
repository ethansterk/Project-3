package code;

public class ColStats {
	private String colName;
	private int minVal;
	private int maxVal;
	
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
