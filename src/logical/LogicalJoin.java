package logical;

import java.util.ArrayList;

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
	private ArrayList<String> leftBaseTables;
	private String rightBaseTable;
	
	public LogicalJoin(LogicalOperator left, LogicalOperator right, Expression condition, ArrayList<String> leftBaseTables, String rightBaseTable) {
		this.left = left;
		this.right = right;
		this.condition = condition;
		this.leftBaseTables = leftBaseTables;
		this.rightBaseTable = rightBaseTable;
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

	public ArrayList<String> getLeftBaseTables() {
		return leftBaseTables;
	}

	public String getRightBaseTable() {
		return rightBaseTable;
	}
	
	@Override
	public ArrayList<String> getBaseTables() {
		ArrayList<String> leftTables = left.getBaseTables();
		ArrayList<String> rightTables = right.getBaseTables();
		ArrayList<String> tables = new ArrayList<String>();
		tables.addAll(leftTables);
		tables.addAll(rightTables);
		return tables;
	}
}
