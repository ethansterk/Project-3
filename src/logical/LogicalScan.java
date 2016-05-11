package logical;

import java.util.ArrayList;

import code.LogicalPlanPrinter;

/**
 * LogicalScan is the logical equivalent of 
 * ScanOperator. It is part of the visitor pattern
 * used in the PhysicalPlanBuilder.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class LogicalScan extends LogicalOperator{
	private String s;
	
	public LogicalScan(String s) {
		this.s = s;
	}
	
	/**
	 * Getter method for the String representing the table to be scanned
	 * in the form "Sailors" or "Sailors AS S"
	 * 
	 * @return s
	 */
	public String getTablename() {
		return s;
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
		ArrayList<String> tables = new ArrayList<String>();
		tables.add(s);
		return tables;
	}

	@Override
	public void accept(LogicalPlanPrinter visitor) {
		visitor.visit(this);
	}

}
