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
	
	public JoinOperator(Operator left, Operator right, Expression condition) {
		this.left = left;
		this.right = right;
		this.condition = condition;
		tLeftCurrent = left.getNextTuple();
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
		Tuple tRight = null;
		Tuple t = null;
		
		if(tLeftCurrent != null) {
			tRight = right.getNextTuple();
			if(tRight != null) {
				if (condition != null) {
					Tuple temp = Tuple.merge(tLeftCurrent, tRight);
					EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(temp);
					condition.accept(visitor);
					if(visitor.getResult()) {
						t = temp;
					}
					else {
						return this.getNextTuple();
					}
				}
				//simple cross-product if no join conditions in WHERE clause
				else {
					t = Tuple.merge(tLeftCurrent, tRight);
				}
			}
			//tRight==null, reached the end of the right child
			else {
				tLeftCurrent = left.getNextTuple();
				//check that the left child hasn't also reached its end
				if (tLeftCurrent == null) 
					return null;
				
				right.reset();
				tRight = right.getNextTuple();	//should be first tuple of right
				if (tRight != null) {
					if (condition != null) {
						Tuple temp = Tuple.merge(tLeftCurrent, tRight);
						EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(temp);
						condition.accept(visitor);
						if(visitor.getResult()) {
							t = temp;
						}
						else
							return this.getNextTuple();
					}
					//simple cross-product if no join conditions in WHERE clause
					else
						t = Tuple.merge(tLeftCurrent, tRight);
				}
			}
		}
		//return t==null if tLeftCurrent==null
		return t;
	}

	@Override
	public void reset() {
		left.reset();
		right.reset();
	}
	
}
