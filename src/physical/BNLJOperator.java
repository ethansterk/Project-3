package physical;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Arrays;

import code.EvaluateExpressionVisitor;
import code.Tuple;
import logical.PhysicalPlanPrinter;

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
	private ArrayList<String> baseTables;
	
	public BNLJOperator(Operator left, Operator right, Expression condition, int bufferSize, ArrayList<String> baseTables) {
		this.left = left;
		this.right = right;
		this.condition = condition;
		this.baseTables = baseTables;
		currentIndex = 0;
		tRightCurrent = right.getNextTuple();	//initialize the first inner relation tuple
		Tuple firstLeft = this.left.getNextTuple();
		int outerRelationNumAttr = 1;
		if (firstLeft != null)
			outerRelationNumAttr = firstLeft.getFields().size();
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
	
	@Override
	public void accept(PhysicalPlanPrinter visitor) {
		visitor.visit(this);
	}
	
	/**
	 * Getter method for left child
	 * @return left
	 */
	public Operator getLeftChild() {
		return left;
	}
	
	/**
	 * Getter method for right child
	 * @return right
	 */
	public Operator getRightChild() {
		return right;
	}

	/**
	 * Getter method for condition
	 * @return condition
	 */
	public Expression getCondition() {
		return condition;
	}

	@Override
	public ArrayList<String> getBaseTables() {
		return baseTables;
	}
}
