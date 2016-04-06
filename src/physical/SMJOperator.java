package physical;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import code.EvaluateExpressionVisitor;
import code.Tuple;

public class SMJOperator extends Operator {

	private Operator R; // relation R
	private Operator S; // relation S
	private Expression condition;
	private Tuple Tr;
	private Tuple Ts;
	private Tuple Gs;
	private ArrayList<String> rSortCols;
	private ArrayList<String> sSortCols;
	private int sPartitionIndex;
	
	public SMJOperator(Operator left, Operator right, Expression condition, ArrayList<String> leftSortCols, ArrayList<String> rightSortCols) {
		R = left;
		S = right;
		this.condition = condition;
		Tr = R.getNextTuple();
		Ts = S.getNextTuple();
		Gs = Ts;
		rSortCols = new ArrayList<String>();
		sSortCols = new ArrayList<String>();
		rSortCols.addAll(leftSortCols);
		sSortCols.addAll(rightSortCols);
		if (rSortCols.size() != sSortCols.size()) {
			System.out.println("I made a horribly wrong assumption.");
		}
		sPartitionIndex = 0;
	}
	
	@Override
	public Tuple getNextTuple() {
		while (Tr != null && Gs != null) {
			System.out.println("Entered SMJ getNextTuple()");
			System.out.println("Tr = " + Tr.tupleString());
			System.out.println("Gs = " + Gs.tupleString());
			while (lesser(Tr,Gs)) {
				System.out.println("Tr < Gs");
				Tr = R.getNextTuple();
				if (Tr == null) return null;
			}
			while (greater(Tr,Gs)) {
				System.out.println("Tr > Gs");
				Gs = S.getNextTuple();
				sPartitionIndex++;
				if (Gs == null) return null;
			}
			Ts = Gs;
			while (equal(Tr,Gs)) {
				System.out.println("Tr = Gs");
				Ts = Gs; //this doesn't really reset it yet...
				S.reset(sPartitionIndex);
				if (equal(Tr,Ts)) {
					Tuple t = Tuple.merge(Tr, Ts);
					Ts = S.getNextTuple();
					sPartitionIndex++;
					EvaluateExpressionVisitor visitor = new EvaluateExpressionVisitor(t);
					condition.accept(visitor);
					if(visitor.getResult()) {
						return t;
					}
					else {
						System.out.println("Something horribly horrible has happened.");
						return null;
					}
				}
				Tr = R.getNextTuple();
			}
			Gs = Ts;
		}
		return null;
	}
	
	private boolean lesser(Tuple tr, Tuple gs) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = gs.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int gsj = Integer.valueOf(gs.getValues().get(sAttrIndex));
			if (tri >= gsj)
				return false;
		}
		return true;
	}
	
	private boolean greater(Tuple tr, Tuple gs) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = gs.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int gsj = Integer.valueOf(gs.getValues().get(sAttrIndex));
			if (tri <= gsj)
				return false;
		}
		return true;
	}
	
	private boolean equal(Tuple tr, Tuple gs) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = gs.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int gsj = Integer.valueOf(gs.getValues().get(sAttrIndex));
			if (tri != gsj)
				return false;
		}
		return true;
	}

	@Override
	public void reset() {
		
	}

}
