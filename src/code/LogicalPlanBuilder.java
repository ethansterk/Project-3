package code;

import java.util.HashMap;
import java.util.List;

import logical.LogicalDuplicateElimination;
import logical.LogicalJoin;
import logical.LogicalOperator;
import logical.LogicalProject;
import logical.LogicalScan;
import logical.LogicalSelect;
import logical.LogicalSort;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * 
 * @author Ryu
 *
 */
public class LogicalPlanBuilder {

	private LogicalOperator root;
	private FromItem fromItem;
	private Expression where;
	private List<SelectItem> selectItems;
	private List<Join> joins;
	private List<OrderByElement> orderByElements;
	private boolean distinct;
	
	/**
	 * Initializes the LogicalPlanBuilder with the necessary information
	 */
	public LogicalPlanBuilder(FromItem fromItem, Expression where, List<SelectItem> selectItems, List<Join> joins, List<OrderByElement> orderByElements, boolean distinct) {
		this.fromItem = fromItem;
		this.where = where;
		this.selectItems = selectItems;
		this.joins = joins;
		this.orderByElements = orderByElements;
		this.distinct = distinct;
		
		createQueryPlan();
	}
	
	/**
	 * Helper method that holds all of the logical plan logic
	 */
	private void createQueryPlan() {
		if (fromItem != null) {
			if (joins != null) {
				
				JoinEvaluateExpressionVisitor joinVisitor = new JoinEvaluateExpressionVisitor(joins);
				if (where != null) where.accept(joinVisitor);
				HashMap<String,Expression> selectConditions = joinVisitor.getSelectConditions();
				HashMap<String,Expression> joinConditions = joinVisitor.getJoinConditions();
				
				String[] wholeTableName = fromItem.toString().split(" ");
				String tableName = wholeTableName[0];
				String aliasName = "";
				if (wholeTableName.length > 1)
					aliasName = wholeTableName[2];
				
				LogicalOperator temp = new LogicalScan(fromItem.toString());
				
				Expression e = selectConditions.get(tableName);
				if (e == null && aliasName != "")
					e = selectConditions.get(aliasName);
				if (e != null)
					temp = new LogicalSelect(temp, e);
				
				//2. Start going through joins
				for (Join j : joins) {
					//2a. Do selection on (j)
					//tempWholeTableName could be "Reserves" or "Reserves AS R"
					String tempWholeTableName = j.getRightItem().toString();
					String[] split = tempWholeTableName.split(" ");
					String tempTableName = split[0];
					String tempAliasName = "";
					if (split.length > 1)
						tempAliasName = split[2];
					
					LogicalOperator tempRight = new LogicalScan(tempWholeTableName);
					Expression ex = selectConditions.get(tempTableName);
					if (ex == null && aliasName != "")
						ex = selectConditions.get(tempAliasName);
					if (ex != null)
						tempRight = new LogicalSelect(tempRight, ex);
					//2b. and 2c. and 2d. Join with temp on applicable join selection; assign as temp
					Expression joinCondition = joinConditions.get(tempTableName);
					if (joinCondition == null && aliasName != "")
						joinCondition = joinConditions.get(tempAliasName);
					temp = new LogicalJoin(temp, tempRight, joinCondition);
				}
				root = temp;
			}
			else {
				root = new LogicalScan(fromItem.toString());
				
				//Check for WHERE conditions
				if (where != null) {
					root = new LogicalSelect(root, where);
					//Check for SELECT projections: "SELECT * FROM ___" or "SELECT x1,... FROM ___"
					root = checkForProjection(root);
				}
			}
			//Check for SELECT projections: "SELECT * FROM ___" or "SELECT x1,... FROM ___"
			root = checkForProjection(root);

			root = checkForSorting(root);
			root = checkForDuplicateEliminating(root);
		}
		else {
			System.err.println("Error: Query needs FROM clause.");
		}
	}
	
	/**
	 * Access method to return the top-most LogicalOperator in the query plan. 
	 * Used by the interpreter/parser to create a physical plan.
	 * 
	 * @return root LogicalOperator
	 */
	public LogicalOperator getRoot() {
		return root;
	}
	
	/**
	 * Simplify whenever we need to check for projections (SELECT clause).
	 * 
	 * @param childOp
	 * @return root node from this perspective
	 */
	private LogicalOperator checkForProjection(LogicalOperator childOp) {
		LogicalOperator temp = null;
		if (selectItems != null) {
			if (!(selectItems.get(0) instanceof AllColumns))
				temp = new LogicalProject(childOp, selectItems);
			else {
				//No WHERE clause or SELECT projections, form of simple select-all queries: "SELECT * FROM __"
				temp = childOp;
			}
		}
		return temp;
	}
	
	/**
	 * Simplify whenever we need to check for sorting (ORDER BY clause).
	 * 
	 * @param childOp
	 * @return root node from this perspective
	 */
	private LogicalOperator checkForSorting(LogicalOperator childOp) {
		LogicalOperator temp = null;
		if (orderByElements != null)
			temp = new LogicalSort(childOp, orderByElements, false);
		else
			temp = childOp;
		
		return temp;
	}
	
	/**
	 * Simplify whenever we need to check for eliminating duplicates (DISTINCT).
	 * 
	 * @param childOp
	 * @return root node from this perspective
	 */
	private LogicalOperator checkForDuplicateEliminating(LogicalOperator childOp) {
		LogicalOperator temp = null;
		if (distinct)
			temp = new LogicalDuplicateElimination(childOp, orderByElements);
		else
			temp = childOp;
		
		return temp;
	}
}
