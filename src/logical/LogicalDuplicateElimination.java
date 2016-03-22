package logical;

import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

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
	
	public List<OrderByElement> getList() {
		return list;
	}

	public LogicalOperator getChild() {
		return child;
	}

	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
