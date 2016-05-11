package logical;

import java.util.ArrayList;

import code.LogicalPlanPrinter;
import net.sf.jsqlparser.expression.Expression;

/**
 * LogicalSelect is the logical equivalent of
 * SelectOperator. It is part of the visitor pattern
 * in the PhysicalPlanBuilder.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LogicalSelect extends LogicalOperator{

	private LogicalOperator child;
	private Expression condition;
	
	public LogicalSelect(LogicalOperator child, Expression condition) {
		this.child = child;	
		this.condition = condition;
	}
	
	/**
	 * Getter method for the child of this operator
	 * @return child
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Getter method for the select condition
	 * @return condition
	 */
	public Expression getCondition() {
		return condition;
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

	@Override
	public void accept(LogicalPlanPrinter visitor) {
		visitor.visit(this);
	}
}
