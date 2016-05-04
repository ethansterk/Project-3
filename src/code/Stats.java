package code;

import java.util.ArrayList;

public class Stats {

	private String relName;
	private int numTuples;
	private ArrayList<ColStats> cols;
	
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

	public void setCols(ArrayList<ColStats> cols) {
		this.cols = cols;
	}
}
