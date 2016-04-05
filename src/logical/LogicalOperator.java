package logical;

import java.util.ArrayList;

/**
 * Basic abstract class that defines the accept method for all logical 
 * operators. Mirrors the Operator class in the physical package.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public abstract class LogicalOperator {

	private ArrayList<String> baseTables = new ArrayList<String>();
	/**
	 * accept method that is used in PhysicalPlanBuilder
	 * @param visitor
	 */
	public abstract void accept(PhysicalPlanBuilder visitor);
	
	public void addBaseTable(String table) {
		baseTables.add(table);
	}
	
	public ArrayList<String> getBaseTables() {
		return baseTables;
	}
}
