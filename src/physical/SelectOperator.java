package physical;
import code.EvaluateExpressionVisitor;
import code.Tuple;
import net.sf.jsqlparser.expression.Expression;
/**
 * SelectOperator is a representation of the relational algebra 
 * operator select. The output of select contains all the tuples
 * of its input that a certain given condition holds for.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */

public class SelectOperator extends Operator {

	private Operator child;
	private Expression condition;
	
	/**
	 * Initializes a select operator.
	 * 
	 * @param child The input of this select operator.
	 * @param condition The condition on this select operator.
	 */
	public SelectOperator(Operator child, Expression condition) {
		this.child = child;	
		this.condition = condition;
	}
	
	/**
	 * Returns the next tuple output from this selection. Uses the
	 * visitor pattern of ExpressionVisitor to evaluate whether
	 * the condition holds on the input tuple.
	 * 
	 * @return t, the next tuple that satisfies the condition
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t;
		while((t = child.getNextTuple()) != null) {
			EvaluateExpressionVisitor v = new EvaluateExpressionVisitor(t);
			condition.accept(v);
			if (v.getResult()) //if condition holds on t
				return t;
		}
		return null;
	}

	@Override
	public void reset() {
		child.reset();
	}
}
