package physical;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import code.OrderComparator;
import code.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * SortOperator is a representation of the output after applying the 
 * ORDER BY clause. The output of a sort contains all the tuples contained in
 * the input, but with certain columns sorted (in ascending order). The
 * list of ORDER BY elements will determine the precedence for sorting. That
 * means that if we have "ORDER BY S.B, S.A", the results will be sorted by
 * S.B first of all, using S.C as a tiebreaker, and then the remaining
 * columns {A, D, ...} as additional tiebreakers, in that order.
 *
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class SortOperator extends Operator {

	private Operator child;
	private ArrayList<String> columns = new ArrayList<String>();
	private ArrayList<Tuple> collection = new ArrayList<Tuple>();
	private int pointer = 0;
	
	public SortOperator(Operator child, List<OrderByElement> list) {
		this.child = child;
		if (list == null)
			list = null;
		else {
			for (OrderByElement x : list) {
				columns.add(x.getExpression().toString());
			}
		}
		createCollection();
	}
	
	public SortOperator(Operator child, ArrayList<String> sortCols) {
		this.child = child;
		if (sortCols == null)
			sortCols = null;
		else {
			columns.addAll(sortCols); // must use addAll() because passes by reference
		}
		createCollection();
	}

	/**
	 * Helper method used to write-to-buffer the contents of the child operator.
	 */
	private void createCollection() {
		//build collection by calling getNextTuple on child until retrieved all tuples
		Tuple t = child.getNextTuple();
		while (t != null) {
				collection.add(t);
				t = child.getNextTuple();
		}
		if(!collection.isEmpty()) {
			ArrayList<String> fields = collection.get(0).getFields();
			OrderComparator oc = new OrderComparator(columns, fields);
			Collections.sort(collection, oc);
		}
	}

	@Override
	public Tuple getNextTuple() {
		//if the user reset on SortOperator, need to reconstruct collection
		if (collection.isEmpty()) 
			createCollection();
		
		Tuple t = null;
		if (hasNextTuple()) {
			Tuple temp = collection.get(pointer);
			if (temp != null)				
				t = temp;
		}
		
		pointer++;
		return t;
	}
	
	/**
	 * Helper method to help us spit out next tuple.
	 * 
	 * @return whether SortOperator is done
	 */
	private boolean hasNextTuple(){
		return pointer < collection.size();
	}

	@Override
	public void reset() {
		pointer = 0;
		collection = new ArrayList<Tuple>();
		child.reset();
	}
	
	@Override
	public void reset(int i) {
		pointer = i;
	}
}
