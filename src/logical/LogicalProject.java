package logical;

import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

public class LogicalProject extends LogicalOperator{

	private LogicalOperator child;
	private List<SelectItem> selectItems;
	
	public LogicalProject(LogicalOperator child, List<SelectItem> selectItems) {
		this.child = child;
		this.selectItems = selectItems;
	}
	
	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}

	public LogicalOperator getChild() {
		return child;
	}

	public List<SelectItem> getSelectItems() {
		return selectItems;
	}
}
