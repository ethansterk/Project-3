package physical;
import java.util.ArrayList;
import java.util.List;

import code.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * DuplicateEliminationOperator is made only when the DISTINCT keyword
 * is present in a SQL query. It is used to eliminate duplicate tuples
 * from its child operator. This operator occurs after all the other
 * operations.
 * If its input is sorted, good; if not, pass the child through SortOperator.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class DuplicateEliminationOperator extends Operator {

	private Operator child;
	private Tuple next;
	
	public DuplicateEliminationOperator(Operator child, List<OrderByElement> list, int extBufferPages) {
		if (child instanceof SortOperator || child instanceof ExternalSortOperator)
			this.child = child;
		else {
			if (extBufferPages >= 1) {
				ArrayList<String> sortList = new ArrayList<String>();
				for (OrderByElement o : list)
					sortList.add(o.getExpression().toString());
				this.child = new ExternalSortOperator(child, extBufferPages, sortList);
			}
			else
				this.child = new SortOperator(child, list);
		}
		next = this.child.getNextTuple();
	}

	/**
	 * Uses the next pointer to implement our ability to "look ahead"
	 * at successive tuples and compare them against our base
	 * Tuple t.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t = next;
		Tuple tempNext = next;
		while (tempNext != null && tempNext.sameAs(t))
			tempNext = child.getNextTuple();
		next = tempNext;
		return t;
	}

	@Override
	public void reset() {
		child.reset();
	}
}
