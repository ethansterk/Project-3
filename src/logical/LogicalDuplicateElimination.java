package logical;

import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * LogicalDuplicateElimination is the logical equivalent of 
 * DuplicateEliminationOperator. It is part of the visitor pattern
 * used in the PhysicalPlanBuilder.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LogicalDuplicateElimination extends LogicalOperator {
	
	private LogicalOperator child;
	private List<OrderByElement> list;
	
	public LogicalDuplicateElimination(LogicalOperator child, List<OrderByElement> list) {
		if (child instanceof LogicalSort)
			this.child = child;
		else
			this.child = new LogicalSort(child, list);
		this.list = list;
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
	 * accept method that is used in PhysicalPlanBuilder
	 * @param visitor
	 */
	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
