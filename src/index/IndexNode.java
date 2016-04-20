package index;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is from CS4320 HW2's skeleton code, with editing.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class IndexNode<K extends Comparable<K>, T> extends Node<K,T> {

	// m nodes
	protected ArrayList<T> leafChildren; // m+1 children
	protected ArrayList<Node<K,T>> indexChildren;

	public IndexNode() {
		isLeafNode = false;
		keys = new ArrayList<K>();
		leafChildren = new ArrayList<T>();
		indexChildren = new ArrayList<Node<K,T>>();
	}

	public IndexNode(List<K> newKeys, List<T> newChildren, List<Node<K,T>> newIndexChildren) {
		isLeafNode = false;
		keys = new ArrayList<K>(newKeys);
		leafChildren = new ArrayList<T>(newChildren);
		indexChildren = new ArrayList<Node<K,T>>(newIndexChildren);
	}
	
	/**
	 * Setter method for keys.
	 * @param keys
	 */
	public void setKeys(ArrayList<K> keys) {
		this.keys = keys;
	}
	
	/**
	 * Setter method for children, if children are LeafNodes.
	 * @param leafChildren
	 */
	public void setLeafChildren(ArrayList<T> children) {
		this.leafChildren = children;
	}
	
	/**
	 * Setter method for children, if children are IndexNodes.
	 * @param indexChildren
	 */
	public void setIndexChildren(ArrayList<Node<K,T>> children) {
		this.indexChildren = children;
	}
	
	/**
	 * Getter method for children, if children are LeafNodes.
	 * @return leafChildren
	 */
	public ArrayList<T> getLeafChildren() {
		return leafChildren;
	}
	
	/**
	 * Getter method for children, if children are IndexNodes.
	 * @return indexChildren
	 */
	public ArrayList<Node<K,T>> getIndexChildren() {
		return indexChildren;
	}
	
	/**
	 * Getter method for the smallest key in the left-most LeafNode of this node's subtree, 
	 * for use in its parent IndexNode's keys.
	 * @return firstKey
	 */
	public int getFirstKey() {
		if (leafChildren.size() != 0)
			return ((LeafNode<K,T>)leafChildren.get(0)).getFirstKey();
		else if (indexChildren.size() != 0)
			return ((IndexNode<K,T>)indexChildren.get(0)).getFirstKey();
		else
			return -1;
	}
}
