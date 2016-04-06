package physical;

import net.sf.jsqlparser.expression.Expression;
import code.Tuple;

public class SMJOperator extends Operator {

	private Operator R; // relation R
	private Operator S; // relation S
	private Expression condition;
	private Tuple Tr;
	private Tuple Ts;
	private Tuple Gs;
	
	public SMJOperator(Operator left, Operator right, Expression condition) {
		R = left;
		S = right;
		this.condition = condition;
		Tr = R.getNextTuple();
		Ts = S.getNextTuple();
		Gs = Ts;
	}
	
	@Override
	public Tuple getNextTuple() {
		while (Tr != null && Gs != null) {
			
		}
		return null;
	}

	@Override
	public void reset() {
		
	}

}
