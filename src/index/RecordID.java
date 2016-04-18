package index;

/**
 * Class specifically for Project 4, for implementing Alternative 3.
 * Each RecordID contains a pageID and tupleID.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class RecordID {

	private int pageID;
	private int tupleID;
	
	public RecordID(int pageID, int tupleID) {
		this.pageID = pageID;
		this.tupleID = tupleID;
	}

	/**
	 * Getter method for this record's pageID.
	 * @return pageID
	 */
	public int getPageID() {
		return pageID;
	}
	
	/**
	 * Getter method for this record's tupleID.
	 * @return tupleID
	 */
	public int getTupleID() {
		return tupleID;
	}
}
