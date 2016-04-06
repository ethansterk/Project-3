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
	private boolean lastWasInPartition;
	
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
		lastWasInPartition = false;
	}
	
	@Override
	public Tuple getNextTuple() {
		while (Tr != null/* && Ts != null*/) {
			if (Ts == null) {
				Tr = R.getNextTuple();
				S.reset(sPartitionIndex);
				Ts = S.getNextTuple();
				//if (Ts == null) return null;
				if (Tr == null) return null;
			}
			if (lastWasInPartition && !equal(Tr,Ts)) {
				Tr = R.getNextTuple();
				S.reset(sPartitionIndex);
				Ts = S.getNextTuple();
			}
			while (lesser(Tr,Ts)) {
				lastWasInPartition = false;
				Tr = R.getNextTuple();
				if (Tr == null) {
					System.out.println("Tr is null");
					return null;
				}
			}
			while (greater(Tr,Ts)) {
				lastWasInPartition = false;
				Ts = S.getNextTuple();
				sPartitionIndex++;
				if (Ts == null){
					System.out.println("Ts is null");
					return null;
				}
			}
			if (equal(Tr,Ts)) {
				lastWasInPartition = true;
				Tuple t = Tuple.merge(Tr, Ts);
				Ts = S.getNextTuple();
				return t;
			}
		}
		System.out.println("t is null");
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
