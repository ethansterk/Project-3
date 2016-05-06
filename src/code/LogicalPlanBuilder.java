package code;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import logical.LogicalDuplicateElimination;
import logical.LogicalJoin;
import logical.LogicalOperator;
import logical.LogicalProject;
import logical.LogicalScan;
import logical.LogicalSelect;
import logical.LogicalSort;
import logical.UnionFind;
import logical.UnionFindElement;
import logical.UnionFindExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * LogicalPlanBuilder constructs an abstract plan for the evaluation of
 * a query. It does not include specific implementation strategies
 * (i.e. which type of join to perform) but it does push selections.
 * The final plan it constructs is made up of LogicalOperators.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
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
				// commented out for P5:
				//JoinEvaluateExpressionVisitor joinVisitor = new JoinEvaluateExpressionVisitor(joins);
				//if (where != null) where.accept(joinVisitor);
				//HashMap<String,Expression> selectConditions = joinVisitor.getSelectConditions();
				//HashMap<String,Expression> joinConditions = joinVisitor.getJoinConditions();
				
				//Initialize the Alias -> TableName HashMap in DBCatalog
				DatabaseCatalog db = DatabaseCatalog.getInstance();
				boolean usesAliases;
				
				String[] wholeTableName = fromItem.toString().split(" ");
				String tableName = wholeTableName[0];
				if (wholeTableName.length > 1) {
					usesAliases = true;
					String aliasName = wholeTableName[2];
					db.addAlias(aliasName, tableName);
				}
				else {
					usesAliases = false;
					db.addAlias(tableName, tableName);
				}
				
				//initialize HashMap from aliases to table names here
				for (Join j : joins) {
					String tempWholeTableName = j.getRightItem().toString();
					String[] split = tempWholeTableName.split(" ");
					String table = split[0];
					if (usesAliases) {
						String alias = split[2];
						db.addAlias(alias, table);
					}
					else {
						db.addAlias(table, table);
					}
				}
				
				UnionFind uf = new UnionFind();
				UnionFindExpressionVisitor ufVisitor = new UnionFindExpressionVisitor(uf);
				if (where != null)
					where.accept(ufVisitor);
				Expression unusable = uf.getUnusable();
				
				JoinEvaluateExpressionVisitor joinVisitor = new JoinEvaluateExpressionVisitor(joins);
				if (unusable != null)
					unusable.accept(joinVisitor);
				HashMap<String,Expression> selectConditions = joinVisitor.getSelectConditions();
				
				LogicalOperator temp = new LogicalScan(fromItem.toString());
				Expression e;
				// TODO left and right base tables must be initialized in the PPBuilder
				// after determining join order
				//ArrayList<String> leftBaseTables = new ArrayList<String>();
				//String rightBaseTable = null;
				if(usesAliases) {
					String aliasName = wholeTableName[2];
					e = selectConditions.get(aliasName);
					// for each column in this table, get attr conditions and append to e
					ArrayList<String> cols = DatabaseCatalog.getInstance().getSchema(tableName).getCols();
					for (String col : cols) {
						String attributeName = aliasName + "." + col;
						UnionFindElement el = uf.find(attributeName);
						if (el.getEqualityConstr() != null) {
							Column c = new Column();
							c.setColumnName(col);
							Table t = new Table();
							c.setTable(new );
							// TODO left off here
							EqualsTo ex = new EqualsTo();
							MyUtils.safeConcatExpression(e, );
						}
					}
					//leftBaseTables.add(aliasName);
				}
				else {
					e = selectConditions.get(tableName);
					//leftBaseTables.add(tableName);
				}
				if (e != null) {
					temp = new LogicalSelect(temp,e);
				}
				
				// Project 5 implementation:
				ArrayList<LogicalOperator> children = new ArrayList<LogicalOperator>();
				children.add(temp);
				Expression joinE = null;
				for (Join j : joins) {
					String tempWholeTableName = j.getRightItem().toString();
					String[] split = tempWholeTableName.split(" ");
					String tempTableName = split[0];
					LogicalOperator tempOp = new LogicalScan(tempWholeTableName);
					Expression selectE;
					if (usesAliases) {
						String tempAliasName = split[2];
						selectE = selectConditions.get(tempAliasName);
						Expression nextJoinCond = joinConditions.get(tempAliasName);
						if (joinE == null)
							joinE = nextJoinCond;
						else if (nextJoinCond != null)
							joinE = new AndExpression(joinE, nextJoinCond);
					}
					else {
						selectE = selectConditions.get(tempTableName);
						Expression nextJoinCond = joinConditions.get(tempTableName);
						if (joinE == null)
							joinE = nextJoinCond;
						else if (nextJoinCond != null)
							joinE = new AndExpression(joinE, nextJoinCond);
					}
					if (selectE != null)
						tempOp = new LogicalSelect(tempOp, selectE);
					
					children.add(tempOp);
				}
				root = new LogicalJoin(children, joinE);
				root = temp;
				
				
				// Project 4 code:
				/*for (Join j : joins) {
					if (rightBaseTable != null)
						leftBaseTables.add(rightBaseTable);
					
					String tempWholeTableName = j.getRightItem().toString();
					String[] split = tempWholeTableName.split(" ");
					String tempTableName = split[0];
					LogicalOperator tempRight = new LogicalScan(tempWholeTableName);
					Expression selectE;
					Expression joinE;
					if (usesAliases) {
						String tempAliasName = split[2];
						selectE = selectConditions.get(tempAliasName);
						joinE = joinConditions.get(tempAliasName);
						rightBaseTable = tempAliasName;
					}
					else {
						selectE = selectConditions.get(tempTableName);
						joinE = joinConditions.get(tempTableName);
						rightBaseTable = tempTableName;
					}
					if (selectE != null)
						tempRight = new LogicalSelect(tempRight, selectE);
					
					ArrayList<String> tempList = new ArrayList<String>();
					tempList.addAll(leftBaseTables);
					
					temp = new LogicalJoin(temp, tempRight, joinE, tempList, rightBaseTable);
				}
				root = temp;*/	
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
