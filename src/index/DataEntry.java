package index;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Class specifically for Project 4, for implementing sorted data entries.
 * Each entry holds a sortKey and a list of recordIDs.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class DataEntry {

	private int sortKey;
	private ArrayList<RecordID> recIDs = new ArrayList<RecordID>();
	
	public DataEntry(int sortKey, ArrayList<RecordID> recIDs) {
		this.sortKey = sortKey;
		this.recIDs = recIDs;
	}

	/**
	 * Getter method for this data entry's sortKey.
	 * @return sortKey
	 */
	public int getSortKey() {
		return sortKey;
	}
	
	/**
	 * Getter method for this data entry's list of RecordIDs.
	 * @return recIDs
	 */
	public ArrayList<RecordID> getRecIDs() {
		return recIDs;
	}
	
	/**
	 * Method to create and insert a new RecordID into the current list.
	 * The new RecordID maintains the sorted order over first pageID, then tupleID.
	 * It is impossible for any two RecordIDs to be the same (pageID, tupleID).
	 * @param pageID
	 * @param tupleID
	 */
	public void insertRecordID(int pageID, int tupleID) {
		int index = 0;
		for (int i = 0; i < recIDs.size(); i++) {
			if (pageID == recIDs.get(i).getPageID()) {
				if (tupleID > recIDs.get(i).getTupleID())
					index = i;
				else
					break;
			}
			else if (pageID > recIDs.get(i).getPageID())
				index = i;
			else
				break;
		}
		recIDs.add(index, new RecordID(pageID, tupleID));
	}
}
