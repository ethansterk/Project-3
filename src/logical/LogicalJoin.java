package logical;

import java.util.ArrayList;

import code.LogicalPlanPrinter;
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

	private ArrayList<LogicalOperator> children;
	private Expression condition;
	private UnionFind uf;
	
	public LogicalJoin(ArrayList<LogicalOperator> children, Expression condition, UnionFind uf) {
		this.children = children;
		this.condition = condition;
		this.uf = uf;
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
	
	@Override
	public ArrayList<String> getBaseTables() {
		ArrayList<String> tables = new ArrayList<String>();
		for(LogicalOperator op : children) {
			tables.addAll(op.getBaseTables());
		}
		return tables;
	}

	@Override
	public void accept(LogicalPlanPrinter visitor) {
		visitor.visit(this);
	}

	public UnionFind getUnionFind() {
		return uf;
	}
}
