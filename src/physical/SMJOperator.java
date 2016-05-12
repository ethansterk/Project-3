package physical;

import java.util.ArrayList;

import code.Tuple;
import logical.PhysicalPlanPrinter;

/**
 * SMJOperator is another alternative to the TNLJ or BNLJ methods. Both its
 * children are assumed to be sorted relations, sorted on the attributes
 * that are included in the join condition. Tuples are retrieved by finding
 * where the meaningful attributes are equal in both relations, defining this
 * is a "partition", and traversing through the partition for each tuple in
 * the left relation with equal meaningful attributes across the partition.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class SMJOperator extends Operator {

	// Note: capitals are used to make our code as clear and similar to the
	// textbook's algorithm on page 460 as possible.
	private Operator R; // relation R
	private Operator S; // relation S
	private Tuple Tr;
	private Tuple Ts;
	private ArrayList<String> rSortCols; // the attributes R is sorted on (size n)
	private ArrayList<String> sSortCols; // the attributes S is sorted on (size n)
	private int sPartitionIndex;
	private boolean wasInPartition;
	private ArrayList<String> baseTables;
	
	/**
	 * 
	 * Constructs a new SMJOperator.
	 * @param left The left child of this join.
	 * @param right This right child of this join.
	 * @param leftSortCols The attributes "left" is sorted on. Order matters.
	 * @param rightSortCols The attributes "right" is sorted on. Order matters
	 */
	public SMJOperator(Operator left, Operator right, ArrayList<String> leftSortCols, ArrayList<String> rightSortCols, ArrayList<String> baseTables) {
		this.baseTables = baseTables;
		R = left;
		S = right;
		Tr = R.getNextTuple();
		Ts = S.getNextTuple();
		rSortCols = new ArrayList<String>();
		sSortCols = new ArrayList<String>();
		rSortCols.addAll(leftSortCols);
		sSortCols.addAll(rightSortCols);
		sPartitionIndex = 0;
		wasInPartition = false;
	}
	
	/**
	 * getNextTuple() retrieves the next non-null tuple if there are tuples
	 * left in the join. Otherwise, it returns null. It works by finding 
	 * where the attributes R and S are sorted on are equal (as required
	 * by the join condition). Tuples this holds true on are said to be
	 * "in the partition". Once this is no longer true, we go through the
	 * partition again, this time for the next tuple in R.
	 */
	@Override
	public Tuple getNextTuple() {
		//looping through while() but never returning
		while (Tr != null) {
			if (Ts == null || wasInPartition && !equal(Tr,Ts)) {
				Tr = R.getNextTuple();
				S.reset(sPartitionIndex);
				Ts = S.getNextTuple();
				if (Tr == null) return null;
				if (Ts == null) return null;
			}
			while (lesser(Tr,Ts)) {
				wasInPartition = false;
				Tr = R.getNextTuple();
				if (Tr == null) {
					return null;
				}
			}
			while (greater(Tr,Ts)) {
				wasInPartition = false;
				Ts = S.getNextTuple();
				sPartitionIndex++;
				if (Ts == null){
					return null;
				}
			}
			if (equal(Tr,Ts)) {
				wasInPartition = true;
				Tuple t = Tuple.merge(Tr, Ts);
				Ts = S.getNextTuple();
				return t;
			}
		}
		return null;
	}
	
	/**
	 * Returns true if tr is less than ts on sort attributes, with
	 * priority given to the first attributes.
	 * @param tr Tuple from relation R (with sort attributes rSortCols).
	 * @param ts Tuple from relation S (with sort attributes sSortCols).
	 * @return true if tr is less than ts on sort attributes, with
	 * priority given to the first attributes; false otherwise
	 */
	private boolean lesser(Tuple tr, Tuple ts) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = ts.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int tsj = Integer.valueOf(ts.getValues().get(sAttrIndex));
			if (i == rSortCols.size() - 1) {
				if (tri >= tsj) return false;
				else return true;
			}
			if (tri > tsj) 
				return false;
			if (tri < tsj)
				return true;
		}
		return false; // no join condition
	}
	
	/**
	 * Returns true if tr is greater than ts on sort attributes, with
	 * priority given to the first attributes.
	 * @param tr Tuple from relation R (with sort attributes rSortCols).
	 * @param ts Tuple from relation S (with sort attributes sSortCols).
	 * @return true if tr is greater than ts on sort attributes, with
	 * priority given to the first attributes; false otherwise
	 */
	private boolean greater(Tuple tr, Tuple ts) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = ts.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int tsj = Integer.valueOf(ts.getValues().get(sAttrIndex));
			if (i == rSortCols.size() - 1) {
				if (tri <= tsj) return false;
				else return true;
			}
			if (tri < tsj)
				return false;
			if (tri > tsj)
				return true;
		}
		return true; // no join condition
	}
	
	/**
	 * Returns true if tr is equal to ts on sort attributes, with
	 * priority given to the first attributes.
	 * @param tr Tuple from relation R (with sort attributes rSortCols).
	 * @param ts Tuple from relation S (with sort attributes sSortCols).
	 * @return true if tr is equal to ts on sort attributes, with
	 * priority given to the first attributes; false otherwise
	 */
	private boolean equal(Tuple tr, Tuple ts) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = ts.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int tsj = Integer.valueOf(ts.getValues().get(sAttrIndex));
			if (tri != tsj)
				return false;
		}
		return true;
	}

	/**
	 * Essentially resets this SMJ operator so that the next tuple retrieved
	 * is the first one that would be retrieved if getNextTuple() had never 
	 * been called before.
	 */
	@Override
	public void reset() {
		R.reset();
		S.reset();
		Tr = R.getNextTuple();
		Ts = S.getNextTuple();
		sPartitionIndex = 0;
		wasInPartition = false;
	}

	@Override
	public void accept(PhysicalPlanPrinter visitor) {
		visitor.visit(this);
	}
	
	/**
	 * Getter method for left child
	 * @return R
	 */
	public Operator getLeftChild() {
		return R;
	}
	
	/**
	 * Getter method for right child
	 * @return S
	 */
	public Operator getRightChild() {
		return S;
	}
	
	public ArrayList<String> getRSortCols() {
		return rSortCols;
	}
	
	public ArrayList<String> getSSortCols() {
		return sSortCols;
	}

	@Override
	public ArrayList<String> getBaseTables() {
		return baseTables;
	}
}
