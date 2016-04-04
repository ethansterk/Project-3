package physical;

import net.sf.jsqlparser.expression.Expression;

import java.util.Arrays;

import code.EvaluateExpressionVisitor;
import code.Logger;
import code.Tuple;

/**
 * BNLJOperator is an alternative to the TNLJ method used in JoinOperator.
 * A block is read in at a time from the outer relation (left); for every
 * tuple of the inner relation (right), we look at every tuple of the
 * outer relation block; if they match on the condition, we return the
 * joined Tuple.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class BNLJOperator extends Operator{

	private Operator left; // outer relation
	private Operator right; // inner relation
	private Expression condition;
	private Tuple[] buffer;
	private int currentIndex;	//keep track of where we are in outer relation buffer
	private Tuple tRightCurrent;	//keep track of where we are in inner relation
	
	public BNLJOperator(Operator left, Operator right, Expression condition, int bufferSize) {
		this.left = left;
		this.right = right;
		this.condition = condition;
		currentIndex = 0;
		tRightCurrent = right.getNextTuple();	//initialize the first inner relation tuple
		int outerRelationNumAttr = left.getNextTuple().getFields().size();
		this.left.reset();	//clear left again
		int numTuples = bufferSize * 4096 / (4 * outerRelationNumAttr);
		buffer = new Tuple[numTuples];
		
		readBlock();
	}
	
	/**
	 * Returns the next tuple as the result of a join. We use block nested
	 * loop join to traverse the outer relation block-by-block, comparing
	 * each inner relation tuple to every tuple in the block. 
	 * We return t as the outer tuple glued to the end of the inner tuple.
	 * 
	 * @return t, the next (joined) tuple satisfying the condition
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t = null;
		
		while (buffer[0] != null) {
			while (tRightCurrent != null) {
				while (currentIndex < buffer.length) {
					Tuple tLeft = buffer[currentIndex++];
					
					if (tLeft != null) {
						t = Tuple.merge(tLeft, tRightCurrent);
						if (condition != null) {
							EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(t);
							condition.accept(visitor);
							if(visitor.getResult()) {
								return t;
							}
							else {
								continue;
							}
						}
						else {
							return t;
						}
					}
					else {
						continue;
					}
				}
				currentIndex = 0;
				tRightCurrent = right.getNextTuple();
			}
			right.reset();
			tRightCurrent = right.getNextTuple();
			readBlock();
		}
		return null;
	}

	@Override
	public void reset() {
		left.reset();
		right.reset();
		Arrays.fill(buffer, null);
		currentIndex = 0;
		tRightCurrent = right.getNextTuple();
		readBlock();
	}

	/**
	 * Reads tuples in from outer relation, filling the buffer.
	 */
	private void readBlock() {
		Arrays.fill(buffer, null);
		for(int i = 0; i < buffer.length; i++) {
			buffer[i] = left.getNextTuple();
			//System.out.println(buffer[i].tupleString());
			if(buffer[i] == null) {
				return;
			}
		}
		return;
	}
}
