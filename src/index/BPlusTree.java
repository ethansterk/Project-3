package index;

import java.util.Stack;

/**
 * This class uses a few elements of our CS4320 HW2 solution.
 * BPlusTree is the data structure for each index in our database.
 * It only deals with bulk-loading, no inserts or deletes.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class BPlusTree<K extends Comparable<K>, T> {

	private Node<K,T> root = new Node<K,T>();
	public static int D;
	
	public BPlusTree (int order) {
		D = order;
	}
	
	/**
	 * Getter method for retrieving the root of this tree.
	 * @return root
	 */
	public Node<K,T> getRoot() {
		return root;
	}

	/**
	 * Getter method for the tree's order
	 * @return order
	 */
	public int getOrder() {
		return D;
	}
	
	/**
	 * When called, it bulkloads the tree index.
	 */
	public void bulkLoad() {
		
	}
}
