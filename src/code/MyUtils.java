package code;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class MyUtils {

	public static Expression safeConcatExpression(Expression e, Expression x) {
		if (e == null)
			e = x;
		else
			e = new AndExpression(e,x);
		return e;
	}
}
