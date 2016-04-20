package logical;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * LogicalSort is the logical equivalent of
 * SortOperator. It is part of the visitor pattern
 * in the PhysicalPlanBuilder.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LogicalSort extends LogicalOperator{
	
	private LogicalOperator child;
	private ArrayList<String> columns = new ArrayList<String>();
	private List<OrderByElement> list;
	private boolean distinct;
	
	public LogicalSort(LogicalOperator child, List<OrderByElement> list, boolean distinct) {
		this.child = child;
		if (list == null)
			list = null;
		else {
			for (OrderByElement x : list) {
				columns.add(x.getExpression().toString());
			}
		}
		this.list = list;
		this.distinct = distinct;
	}
	
	/**
	 * Getter method for the OrderByElement list
	 * @return list
	 */
	public List<OrderByElement> getList() {
		return list;
	}

	/**
	 * Getter method for the child of this operator
	 * @return child
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Getter method for the ArrayList representation
	 * of the columns to be sorted by
	 * @return columns
	 */
	public ArrayList<String> getColumns() {
		return columns;
	}
	
	/**
	 * Getter method for the boolean telling us if this sortop is
	 * the result of a DISTINCT keyword.
	 * @return distinct
	 */
	public boolean getDistinct() {
		return distinct;
	}

	/**
	 * accept method that is used in PhysicalPlanBuilder
	 * @param visitor
	 */
	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
	
	@Override
	public ArrayList<String> getBaseTables() {
		return child.getBaseTables();
	}
}
