package logical;

import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * The UnionFindExpressionVisitor is used to visit an expression and 
 * initialize a UnionFind data structure so that is has elements that
 * correspond with the expression and the base tables.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class UnionFindExpressionVisitor implements ExpressionVisitor {

	private UnionFind uf;
	
	private Stack<Integer> vals; // collect integer operands
	private Stack<UnionFindElement> elements; // collect column operands
	
	public UnionFindExpressionVisitor(UnionFind uf) {
		this.uf = uf;
		
		vals = new Stack<Integer>();
		elements = new Stack<UnionFindElement>();
	}
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		vals.add((int) (arg0.getValue()));
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int numVals = vals.size();
		switch(numVals) {
		case 0:
			UnionFindElement ufe1 = elements.pop();
			UnionFindElement ufe2 = elements.pop();
			uf.union(ufe1, ufe2);
			break;
		case 1:
			int equalityVal = vals.pop();
			UnionFindElement ufe = elements.pop();
			ufe.setEqualityConstr(equalityVal);
			break;
		}
	}

	@Override
	public void visit(GreaterThan arg0) {
		boolean valIsLeft = false;
		arg0.getLeftExpression().accept(this);
		if (vals.size() > 0)
			valIsLeft = true;
		arg0.getRightExpression().accept(this);
		int numVals = vals.size();
		switch(numVals) {
		case 0:
			uf.addToUnusable(arg0);
			break;
		case 1:
			if (valIsLeft) { 	// 3 > R.A
				int highBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfHighBound(highBound - 1);
			}
			else {				// R.A > 3
				int lowBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfLowBound(lowBound + 1);
			}
			break;
		}
		vals.clear();
		elements.clear();
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		boolean valIsLeft = false;
		arg0.getLeftExpression().accept(this);
		if (vals.size() > 0)
			valIsLeft = true;
		arg0.getRightExpression().accept(this);
		int numVals = vals.size();
		switch(numVals) {
		case 0:
			uf.addToUnusable(arg0);
			break;
		case 1:
			if (valIsLeft) { 	// 3 >= R.A
				int highBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfHighBound(highBound);
			}
			else {				// R.A >= 3
				int lowBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfLowBound(lowBound);
			}
			break;
		}
		vals.clear();
		elements.clear();
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan arg0) {
		boolean valIsLeft = false;
		arg0.getLeftExpression().accept(this);
		if (vals.size() > 0)
			valIsLeft = true;
		arg0.getRightExpression().accept(this);
		int numVals = vals.size();
		switch(numVals) {
		case 0:
			uf.addToUnusable(arg0);
			break;
		case 1:
			if (valIsLeft) { 	// 3 < R.A
				int lowBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfLowBound(lowBound + 1);
			}
			else {				// R.A < 3
				int highBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfHighBound(highBound - 1);
			}
			break;
		}
		vals.clear();
		elements.clear();
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		boolean valIsLeft = false;
		arg0.getLeftExpression().accept(this);
		if (vals.size() > 0)
			valIsLeft = true;
		arg0.getRightExpression().accept(this);
		int numVals = vals.size();
		switch(numVals) {
		case 0:
			uf.addToUnusable(arg0);
			break;
		case 1:
			if (valIsLeft) { 	// 3 <= R.A
				int lowBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfLowBound(lowBound);
			}
			else {				// R.A <= 3
				int highBound = vals.pop();
				UnionFindElement ufe = elements.pop();
				ufe.setIfHighBound(highBound);
			}
			break;
		}
		vals.clear();
		elements.clear();
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		uf.addToUnusable(arg0);
	}

	@Override
	public void visit(Column arg0) {
		String attributeName = "";
		String tablename = arg0.getTable().getAlias(); // TODO check this (should be R, S, etc)
		if (tablename == null)
			tablename = arg0.getTable().getName();
		String colname = arg0.getColumnName();
		attributeName = tablename + "." + colname;
		
		UnionFindElement el = uf.find(attributeName);
		elements.add(el);
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

}
