import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;
/**
 * JoinEvaluateExpressionVisitor is an ExpressionVisitor thats
 * purpose is to break down and evaluate a selection condition
 * that may contain references to more than one table. It does this
 * by partitioning the WHERE clause on "AND" and deciding if the 
 * expression is a normal select or join condition.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class JoinEvaluateExpressionVisitor implements ExpressionVisitor{

	//maps a table name to its select condition
	private HashMap<String,Expression> selectConditions;
	//maps the table name that gets joined last to its select condition
	private HashMap<String,Expression> joinConditions;
	//stores the names of tables for recent subexpression traversals.
	private ArrayList<String> tableNames;
	//stores the joins tables, preserving the order in which they are joined
	private ArrayList<String> joinList;
	
	/**
	 * Returns the select conditions.
	 * 
	 * @return selectConditions
	 */
	public HashMap<String, Expression> getSelectConditions() {
		return selectConditions;
	}

	/**
	 * Returns the join conditions.
	 * 
	 * @return joinConditions
	 */
	public HashMap<String, Expression> getJoinConditions() {
		return joinConditions;
	}

	/**
	 * Initializes the select and join condition data structures.
	 * 
	 * @param joins 
	 */
	public JoinEvaluateExpressionVisitor(List<Join> joins) {
		selectConditions = new HashMap<String,Expression>();
		joinConditions = new HashMap<String,Expression>();
		tableNames = new ArrayList<String>();
		joinList = new ArrayList<String>();
		for(Join j: joins) {
			//tempWholeTableName could be "Reserves" or "Reserves AS R"
			String tableName = j.getRightItem().toString();
			String[] split = tableName.split(" ");
			if (split.length > 1)
				tableName = split[2];
			else {
				tableName = split[0];
			}
			joinList.add(tableName);
		}
	}
	
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		// do nothing
	}
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	
	/**
	 * Visits an AND expression. The expression has two subexpressions.
	 * They are handled separately. If the subexpression is itself an
	 * AndExpression, then this subexpression should not be added to
	 * the select/join conditions instance variables as this is done
	 * recursively. If the subexpression is not an AndExpression, then
	 * it is added to the appropriate select/join condition.
	 */
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		handleExpression(arg0);
	}
	
	/**
	 * Visits a column value. Extracts the table name from the column.
	 */
	public void visit(Column arg0) {
		String tablename = arg0.getTable().getName();
		if(!tableNames.contains(tablename))
			tableNames.add(tablename);
		// TODO Auto-generated method stub
		
	}
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * 
	 * 
	 * @param e
	 */
	private void handleExpression(Expression e) {
		switch(tableNames.size()) {
		case 1:
			String table = tableNames.remove(0);
			if(selectConditions.get(table) != null) {
				Expression oldE = selectConditions.get(table);
				AndExpression newE = new AndExpression(oldE, e);
				selectConditions.put(table, newE);
			}
			else {
				selectConditions.put(table, e);
			}
			break;
		case 2:
			//get the tableName of higher height in left-deep tree (gets joined later)
			String tableA = tableNames.remove(0);
			String tableB = tableNames.remove(0);
			int heightA = joinList.indexOf(tableA);
			int heightB = joinList.indexOf(tableB);
			String higherTable;
			if(heightA > heightB) {
				higherTable = tableA;
			}
			else {
				higherTable = tableB;
			}
			//create map between that table and the expression
			if(joinConditions.get(higherTable) != null) {
				Expression oldE = joinConditions.get(higherTable);
				AndExpression newE = new AndExpression(oldE, e);
				joinConditions.put(higherTable, newE);
			}
			else {
				joinConditions.put(higherTable, e);
			}
			tableNames.clear();
			break;
		default:
			throw new IllegalStateException("tableNames has " + tableNames.size() + " elements");
		}
	}
}
