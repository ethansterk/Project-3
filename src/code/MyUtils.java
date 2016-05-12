package code;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/**
 * This class was meant to contain useful utlity functions, but we
 * created it kind of late so there is only one LOL.
 * 
 * @author Laura Ng (ln233) and Ethan Sterk (ejs334)
 *
 */
public class MyUtils {

	/**
	 * Concatenates an Expression onto another while avoid null exceptions.
	 * @param e
	 * @param x
	 * @return
	 */
	public static Expression safeConcatExpression(Expression e, Expression x) {
		if (e == null)
			e = x;
		else
			e = new AndExpression(e,x);
		return e;
	}
}
