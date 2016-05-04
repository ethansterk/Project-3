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

	//private LogicalOperator left;
	//private LogicalOperator right;
	private ArrayList<LogicalOperator> children;
	private Expression condition;
	//private ArrayList<String> leftBaseTables;
	//private String rightBaseTable;
	
	public LogicalJoin(ArrayList<LogicalOperator> children, Expression condition) {
		this.children = children;
		this.condition = condition;
		//this.leftBaseTables = leftBaseTables;
		//this.rightBaseTable = rightBaseTable;
	}

	/**
	 * Getter method for the children of this operator
	 * @return children
	 */
	public ArrayList<LogicalOperator> getChildren() {
		return children;
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

	/*public ArrayList<String> getLeftBaseTables() {
		return leftBaseTables;
	}

	public String getRightBaseTable() {
		return rightBaseTable;
	}*/
	
	@Override
	public ArrayList<String> getBaseTables() {
		ArrayList<ArrayList<String>> childrenTables = new ArrayList<ArrayList<String>>();
		for(LogicalOperator op : children) {
			childrenTables.add(op.getBaseTables());
		}
		ArrayList<String> tables = new ArrayList<String>();
		for(ArrayList<String> childBaseTable : childrenTables) {
			tables.addAll(childBaseTable);
		}
		return tables;
	}
}
