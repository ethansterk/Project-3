package logical;

import net.sf.jsqlparser.expression.Expression;

public class LogicalSelect extends LogicalOperator{

	private LogicalOperator child;
	private Expression condition;
	
	public LogicalSelect(LogicalOperator child, Expression condition) {
		this.child = child;	
		this.condition = condition;
	}
	
	public LogicalOperator getChild() {
		return child;
	}

	public Expression getCondition() {
		return condition;
	}

	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
