package index;
import java.util.ArrayList;

/**
 * This class is from CS4320 HW2's skeleton code.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 */
public class Node<K extends Comparable<K>, T> {
	protected boolean isLeafNode;
	protected ArrayList<K> keys;
	protected int address;

	public boolean isOverflowed() {
		return keys.size() > 2 * BPlusTree.D;
	}

	public boolean isUnderflowed() {
		return keys.size() < BPlusTree.D;
	}
	
	/**
	 * Getter method for keys
	 * @return keys
	 */
	public ArrayList<K> getKeys() {
		return keys;
	}
	
	/**
	 * Getter method for address.
	 * @return address
	 */
	public int getAddress() {
		return address;
	}
	
	/**
	 * Setter method for address.
	 * @param address
	 */
	public void setAddress(int address) {
		this.address = address;
	}
}
