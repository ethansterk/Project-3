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

	public boolean isOverflowed() {
		return keys.size() > 2 * BPlusTree.D;
	}

	public boolean isUnderflowed() {
		return keys.size() < BPlusTree.D;
	}

}
