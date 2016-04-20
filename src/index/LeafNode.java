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
	
	/**
	 * Getter method for values
	 * @return values
	 */
	public ArrayList<T> getValues() {
		return values;
	}
	
	/**
	 * Getter method for the smallest key in this LeafNode's keys, 
	 * for use in its parent IndexNode's keys.
	 * @return firstKey
	 */
	public K getFirstKey() {
		return keys.get(0);
	}
}
