package physical;
import code.EvaluateExpressionVisitor;
import code.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * JoinOperator is a representation of the relational algebra operator
 * join. The output of a join contains the tuples contained in
 * both the left child and the right child that satisfy a join
 * condition.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class JoinOperator extends Operator {

	private Operator left;
	private Operator right;
	private Expression condition;						//if null, it's a cross-product
	private Tuple tLeftCurrent;		//current Tuple in left child, starts at first tuple
	private Tuple tRight;
	
	public JoinOperator(Operator left, Operator right, Expression condition) {
		this.left = left;
		this.right = right;
		this.condition = condition;
	}
	
	/**
	 * Returns the next tuple as the result of a join. We use tuple nested
	 * loop, which scans all of the right (inner) child completely for every tuple 
	 * of the left (outer) child. We return t as the outer tuple glued to
	 * the end of the inner tuple.
	 * 
	 * @return t, the next (joined) tuple satisfying the condition
	 */
	@Override
	public Tuple getNextTuple() {
		if (tLeftCurrent == null)
			tLeftCurrent = left.getNextTuple();
		while(tLeftCurrent != null) {
			tRight = right.getNextTuple();
			if(tRight != null) {
				Tuple t = null;
				if (condition != null) {
					t = Tuple.merge(tLeftCurrent, tRight);
					EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(t);
					condition.accept(visitor);
					if(visitor.getResult()) {
						return t;
					}
					else {
						continue;
					}
				}
				//simple cross-product if no join conditions in WHERE clause
				else {
					t = Tuple.merge(tLeftCurrent, tRight);
					return t;
				}
			}
			else {
				tLeftCurrent = left.getNextTuple();
				right.reset();
			}
		}
		return null;

	}

	@Override
	public void reset() {
		left.reset();
		right.reset();
		tLeftCurrent = null;
		tRight = null;
	}
	
}