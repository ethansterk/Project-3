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

	/**
	 * accept method that is used in PhysicalPlanBuilder
	 * @param visitor
	 */
	public abstract void accept(PhysicalPlanBuilder visitor);
	
	/**
	 * Returns a list of relations that this operator has as leaves.
	 * @return List of relations.
	 */
	public abstract ArrayList<String> getBaseTables();
	
}
