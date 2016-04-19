package index;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is from CS4320 HW2's skeleton code, with editing.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LeafNode<K extends Comparable<K>, T> extends Node<K, T> {
	protected ArrayList<T> values;
	protected LeafNode<K,T> nextLeaf;
	protected LeafNode<K,T> previousLeaf;
	
	/**
	 * Added this general constructor
	 */
	public LeafNode() {
		isLeafNode = true;
		keys = new ArrayList<K>();
		values = new ArrayList<T>();
	}
	public LeafNode(List<K> newKeys, List<T> newValues) {
		isLeafNode = true;
		keys = new ArrayList<K>(newKeys);
		values = new ArrayList<T>(newValues);
	}
	
	/**
	 * Setter method for keys.
	 * @param keys
	 */
	public void setKeys(ArrayList<K> keys) {
		this.keys = keys;
	}
	
	/**
	 * Setter method for values.
	 * @param values
	 */
	public void setValues(ArrayList<T> values) {
		this.values = values;
	}
}
