package physical;

import net.sf.jsqlparser.expression.Expression;
import code.Tuple;

public class BNLJOperator extends Operator{

	private Operator left; // outer relation
	private Operator right; // inner relation
	private Expression condition;
	private Tuple[] buffer;
	
	public BNLJOperator(Operator left, Operator right, Expression condition, int bufferSize) {
		this.left = left;
		this.right = right;
		this.condition = condition;
		int outerRelationNumAttr = 0; // TODO -- How to retrieve number attributes?
		int numTuples = bufferSize * 4096 / (4 * outerRelationNumAttr);
		buffer = new Tuple[numTuples];
		
		readBlock();
	}
	
	@Override
	public Tuple getNextTuple() {
		
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * Reads tuples in from outer relation, filling the buffer.
	 */
	private void readBlock() {
		int i = 0;
		while(i < buffer.length) {
			buffer[i] = left.getNextTuple();
			if(buffer[i] == null) {
				return;
			}
		}
	}
}
