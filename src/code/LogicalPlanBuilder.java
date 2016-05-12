package code;

import java.io.StringReader;
import java.util.ArrayList;
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
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
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
				DatabaseCatalog db = DatabaseCatalog.getInstance();
				db.setJoins(joins);
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
				
				LogicalOperator temp = new LogicalScan(fromItem.toString());
				Expression e = null;

				e = appendUnionConstraints(e,wholeTableName,uf);
				
				if (e != null) {
					temp = new LogicalSelect(temp,e);
				}
				
				// Project 5 implementation:
				ArrayList<LogicalOperator> children = new ArrayList<LogicalOperator>();
				children.add(temp);
				
				for (Join j : joins) {
					String tempWholeTableName = j.getRightItem().toString();
					String[] split = tempWholeTableName.split(" ");
					LogicalOperator tempOp = new LogicalScan(tempWholeTableName);
					Expression selectE = null;

					selectE = appendUnionConstraints(selectE, split, uf);
					
					if (selectE != null)
						tempOp = new LogicalSelect(tempOp, selectE);
					
					children.add(tempOp);
				}
				root = new LogicalJoin(children, unusable, uf);
				//root = temp;
			}
			else {
				root = new LogicalScan(fromItem.toString());
				
				//Check for WHERE conditions
				if (where != null) {
					root = new LogicalSelect(root, where);
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
	
	private Expression appendUnionConstraints(Expression e, String[] wholeTableName, UnionFind uf) {
		String relName;
		String tableName = wholeTableName[0];
		if (wholeTableName.length > 1)
			relName = wholeTableName[2];
		else
			relName = wholeTableName[0];
		// for each column in this table, get attr conditions and append to e
		ArrayList<UnionFindElement> usedElements = new ArrayList<UnionFindElement>();
		// avoid using same element (with same condition) twice for one relation
		ArrayList<String> cols = DatabaseCatalog.getInstance().getSchema(tableName).getCols();
		for (String col : cols) {
			String attributeName = relName + "." + col;
			
			UnionFindElement el = uf.find(attributeName);
			if (usedElements.contains(el))
				continue;
			usedElements.add(el);
			
			Expression elExpr = null;
			Integer eq = el.getEqualityConstr();
			if (eq != null) {
				String stringE = attributeName + "=" + eq;
				elExpr = createExpressionFromString(elExpr, stringE);
				if (elExpr != null) {
					e = MyUtils.safeConcatExpression(e, elExpr);
				}
			}
			else {
				Integer low = el.getLowBound();
				Integer high = el.getHighBound();
				if (low != null) {
					String stringE = attributeName + ">=" + low;
					elExpr = createExpressionFromString(elExpr, stringE);
				}
				if (elExpr != null)
					e = MyUtils.safeConcatExpression(e, elExpr);
				elExpr = null;
				if (high != null) {
					String stringE = attributeName + "<=" + high;
					elExpr = createExpressionFromString(elExpr, stringE);
				}
				if (elExpr != null) {
					e = MyUtils.safeConcatExpression(e, elExpr);
				}
			}
			ArrayList<String> attrs = el.getAttributes();
			for (String att : attrs) {
				String[] split = att.split("\\."); // [R,A]
				if (relName.equals(split[0]) && !col.equals(split[1])) {
					String stringE = attributeName + "=" + att;
					elExpr = createExpressionFromString(elExpr, stringE);
					e = MyUtils.safeConcatExpression(e, elExpr);
				}
			}
		}
		return e;
	}

	private Expression createExpressionFromString(Expression e, String stringE) {
		CCJSqlParser parser = new CCJSqlParser(new StringReader(stringE));
		try {
			e = parser.Expression();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		return e;
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
