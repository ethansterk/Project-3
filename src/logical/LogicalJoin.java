package logical;

import net.sf.jsqlparser.expression.Expression;

/**
 * LogicalJoin is the logical equivalent of 
 * JoinOperator. It is part of the visitor pattern
 * used in the PhysicalPlanBuilder.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LogicalJoin extends LogicalOperator{

	private LogicalOperator left;
	private LogicalOperator right;
	private Expression condition;
	
	public LogicalJoin(LogicalOperator left, LogicalOperator right, Expression condition) {
		this.left = left;
		this.right = right;
		this.condition = condition;
	}

	/**
	 * Getter method for the left-child of this operator
	 * @return left
	 */
	public LogicalOperator getLeft() {
		return left;
	}

	/**
	 * Getter method for the right-child of this operator
	 * @return right
	 */
	public LogicalOperator getRight() {
		return right;
	}

	/**
	 * Getter method for the join condition
	 * @return condition
	 */
	public Expression getCondition() {
		return condition;
	}
	
	/**
	 * accept method that is used by PhysicalPlanBuilder
	 * @param visitor
	 */
	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
