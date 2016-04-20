package logical;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * LogicalProject is the logical equivalent of 
 * ProjectOperator. It is part of the visitor pattern
 * used in the PhysicalPlanBuilder.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LogicalProject extends LogicalOperator{

	private LogicalOperator child;
	private List<SelectItem> selectItems;
	
	public LogicalProject(LogicalOperator child, List<SelectItem> selectItems) {
		this.child = child;
		this.selectItems = selectItems;
	}
	
	/**
	 * Getter method for the child of this operator
	 * @return child
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Getter method for the SelectItem list
	 * @return selectItems
	 */
	public List<SelectItem> getSelectItems() {
		return selectItems;
	}

	/**
	 * accept method that is used in PhysicalPlanBuilder
	 * @param visitor
	 */
	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
	
	@Override
	public ArrayList<String> getBaseTables() {
		return child.getBaseTables();
	}
}
