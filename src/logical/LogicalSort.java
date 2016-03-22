package logical;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class LogicalSort extends LogicalOperator{
	
	private LogicalOperator child;
	private ArrayList<String> columns = new ArrayList<String>();
	private List<OrderByElement> list;
	
	public LogicalSort(LogicalOperator child, List<OrderByElement> list) {
		this.child = child;
		if (list == null)
			list = null;
		else {
			for (OrderByElement x : list) {
				columns.add(x.getExpression().toString());
			}
		}
		this.list = list;
	}
	
	public List<OrderByElement> getList() {
		return list;
	}

	public LogicalOperator getChild() {
		return child;
	}

	public ArrayList<String> getColumns() {
		return columns;
	}

	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
