package code;
import java.util.HashMap;
import java.util.List;

import physical.DuplicateEliminationOperator;
import physical.JoinOperator;
import physical.Operator;
import physical.ProjectOperator;
import physical.ScanOperator;
import physical.SelectOperator;
import physical.SortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * OBSOLETE CLASS AS OF PROJECT 3
 * The QueryPlan class implements the construction of a
 * query plan--a tree-like representation of the operators'
 * relation to each other. This class's purpose is to 
 * return the root operator to the Parse class so that
 * root.dump() will return the right query answers.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class QueryPlan {
	
	private Operator root;
	private FromItem fromItem;
	private Expression where;
	private List<SelectItem> selectItems;
	private List<Join> joins;
	private List<OrderByElement> orderByElements;
	private boolean distinct;

	public QueryPlan(FromItem fromItem, Expression where, List<SelectItem> selectItems, List<Join> joins, List<OrderByElement> orderByElements, boolean distinct) {
		this.fromItem = fromItem;
		this.where = where;
		this.selectItems = selectItems;
		this.joins = joins;
		this.orderByElements = orderByElements;
		this.distinct = distinct;
		
		createQueryPlan();
	}
	
	/**
	 * Helper method that holds all of the query plan logic
	 */
	private void createQueryPlan() {
		//created bottom up, with ScanOperator on the outermost loop
		
		//must have a FROM clause
		if (fromItem != null) {
			if (joins != null) {
				JoinEvaluateExpressionVisitor joinVisitor = new JoinEvaluateExpressionVisitor(joins);
				if (where != null) where.accept(joinVisitor);
				//these get applied after scan and before join
				HashMap<String,Expression> selectConditions = joinVisitor.getSelectConditions();
				//these get applied after specified joins
				HashMap<String,Expression> joinConditions = joinVisitor.getJoinConditions();
				
				//1. Do selection (using selectConditions) on R if applicable; assign as temp
				String[] wholeTableName = fromItem.toString().split(" ");
				String tableName = wholeTableName[0];
				String aliasName = "";
				if (wholeTableName.length > 1)
					aliasName = wholeTableName[2];
				Operator temp = new ScanOperator(fromItem.toString());
				Expression e = selectConditions.get(tableName);
				if (e == null && aliasName != "")
					e = selectConditions.get(aliasName);
				if (e != null)
					temp = new SelectOperator(temp, e);
				
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
					
					Operator tempRight = new ScanOperator(tempWholeTableName);
					Expression ex = selectConditions.get(tempTableName);
					if (ex == null && aliasName != "")
						ex = selectConditions.get(tempAliasName);
					if (ex != null)
						tempRight = new SelectOperator(tempRight, ex);
					//2b. and 2c. and 2d. Join with temp on applicable join selection; assign as temp
					Expression joinCondition = joinConditions.get(tempTableName);
					if (joinCondition == null && aliasName != "")
						joinCondition = joinConditions.get(tempAliasName);
					temp = new JoinOperator(temp, tempRight, joinCondition);
				}
				root = temp;
			}
			else {
				root = new ScanOperator(fromItem.toString());
				
				//Check for WHERE conditions
				if (where != null) {
					root = new SelectOperator(root, where);
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
	 * Access method to return the top-most Operator in the query plan. 
	 * Used by the interpreter/parser to call dump() on.
	 * 
	 * @return root Operator
	 */
	public Operator getRoot() {
		return root;
	}
	
	/**
	 * Simplify whenever we need to check for projections (SELECT clause).
	 * 
	 * @param childOp
	 * @return root node from this perspective
	 */
	private Operator checkForProjection(Operator childOp) {
		Operator temp = null;
		if (selectItems != null) {
			if (!(selectItems.get(0) instanceof AllColumns))
				temp = new ProjectOperator(childOp, selectItems);
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
	private Operator checkForSorting(Operator childOp) {
		Operator temp = null;
		if (orderByElements != null)
			temp = new SortOperator(childOp, orderByElements);
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
	private Operator checkForDuplicateEliminating(Operator childOp) {
		Operator temp = null;
		if (distinct)
			temp = new DuplicateEliminationOperator(childOp, orderByElements, 0);
		else
			temp = childOp;
		
		return temp;
	}
}
