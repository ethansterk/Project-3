package code;
import java.util.Stack;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
/**
 * EvaluateExpressionVisitor is an ExpressionVisitor thats
 * purpose is to break down and evaluate a select condition
 * (represented as an Expression) to a boolean that represents
 * whether this condition holds on a given tuple.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class EvaluateExpressionVisitor implements ExpressionVisitor{

	private Stack<Boolean> valid = new Stack<Boolean>();
	private Stack<Integer> operands = new Stack<Integer>();
	private Tuple t;
	
	/**
	 * Initializes the visitor with a given Tuple.
	 * @param t the Tuple.
	 */
	public EvaluateExpressionVisitor(Tuple t) {
		this.t = t;
	}
	
	/**
	 * Returns the result, whether or not the condition holds for
	 * the Tuple t.
	 * @return true if the condition holds, false if it does not.
	 */
	public boolean getResult() {
		return valid.peek();
	}
	
	public void visit(NullValue arg0) {
		
	}

	public void visit(Function arg0) {
		
	}

	public void visit(InverseExpression arg0) {
		
	}

	public void visit(JdbcParameter arg0) {
		
	}

	public void visit(DoubleValue arg0) {
		
	}

	/**
	 * Visits a value. This can be thought of as a leaf of
	 * the Expression we are evaluating.
	 * Note: Integers are expressed as Longs here.
	 */
	public void visit(LongValue arg0) {
		operands.push((int)arg0.getValue());
	}

	public void visit(DateValue arg0) {
		
	}

	public void visit(TimeValue arg0) {
		
	}

	public void visit(TimestampValue arg0) {
		
	}

	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	public void visit(StringValue arg0) {
		
	}

	/**
	 * Visits a + operator in the condition. Adds the addition
	 * of the left and right expressions to the operands stack.
	 */
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		operands.push(operands.pop() + operands.pop());
	}

	/**
	 * Visits a / operator in the condition. Adds the division
	 * of the left and right expressions to the operands stack.
	 */
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		operands.push(left / right);
	}

	/**
	 * Visits a * operator in the condition. Adds the multiplication
	 * of the left and right expressions to the operands stack.
	 */
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		operands.push(operands.pop() * operands.pop());
	}

	/**
	 * Visits a - operator in the condition. Adds the subtraction
	 * of the left and right expressions to the operands stack.
	 */
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		operands.push(left - right);
	}

	/**
	 * Visits a AND operator in the condition. Adds the && of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		valid.push(valid.pop() && valid.pop());
	}

	/**
	 * Visits an OR operator in the condition. Adds the || of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean one = valid.pop();
		boolean two = valid.pop();
		valid.push(one || two);
	}

	public void visit(Between arg0) {
		
	}

	/**
	 * Visits a = operator in the condition. Adds the .equals() of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		valid.push(operands.pop().equals(operands.pop()));
	}

	/**
	 * Visits a > operator in the condition. Adds the > of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(GreaterThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		valid.push(left > right);
	}

	/**
	 * Visits a >= operator in the condition. Adds the >= of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		valid.push(left >= right);
	}

	public void visit(InExpression arg0) {
		
	}

	public void visit(IsNullExpression arg0) {
		
	}

	public void visit(LikeExpression arg0) {
		
	}

	/**
	 * Visits a < operator in the condition. Adds the < of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		valid.push(left < right);
	}

	/**
	 * Visits a <= operator in the condition. Adds the <= of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		valid.push(left <= right);
	}

	/**
	 * Visits a != operator in the condition. Adds the != of the
	 * left and right expressions to the valid result stack.
	 */
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		int right = operands.pop();
		int left = operands.pop();
		valid.push(left != right);
	}

	/**
	 * Visits a column operator in the condition. This can be thought of 
	 * as a leaf of the Expression we are evaluating.
	 */
	public void visit(Column arg0) {
		String colname = arg0.getWholeColumnName();
		int colpos = t.getFields().indexOf(colname);
		operands.push(Integer.valueOf(t.getAttribute(colpos)));
	}

	public void visit(SubSelect arg0) {
		
	}

	public void visit(CaseExpression arg0) {
		
	}

	public void visit(WhenClause arg0) {
		
	}

	public void visit(ExistsExpression arg0) {
		
	}

	public void visit(AllComparisonExpression arg0) {
		
	}

	public void visit(AnyComparisonExpression arg0) {
		
	}

	public void visit(Concat arg0) {
		
	}

	public void visit(Matches arg0) {
		
	}

	public void visit(BitwiseAnd arg0) {
		
	}

	public void visit(BitwiseOr arg0) {
		
	}

	public void visit(BitwiseXor arg0) {
		
	}

}
