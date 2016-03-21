package logical;

import net.sf.jsqlparser.expression.Expression;

public class LogicalJoin extends LogicalOperator{

	private LogicalOperator left;
	private LogicalOperator right;
	private Expression condition;
	
	public LogicalJoin(LogicalOperator left, LogicalOperator right, Expression condition) {
		this.left = left;
		this.right = right;
		this.condition = condition;
	}
	
	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
