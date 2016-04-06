package physical;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import code.EvaluateExpressionVisitor;
import code.Tuple;

public class SMJOperator extends Operator {

	private Operator R; // relation R
	private Operator S; // relation S
	private Tuple Tr;
	private Tuple Ts;
	private ArrayList<String> rSortCols;
	private ArrayList<String> sSortCols;
	private int sPartitionIndex;
	private boolean lastWasInPartition;
	
	public SMJOperator(Operator left, Operator right, ArrayList<String> leftSortCols, ArrayList<String> rightSortCols) {
		R = left;
		S = right;
		Tr = R.getNextTuple();
		Ts = S.getNextTuple();
		rSortCols = new ArrayList<String>();
		sSortCols = new ArrayList<String>();
		rSortCols.addAll(leftSortCols);
		sSortCols.addAll(rightSortCols);
		sPartitionIndex = 0;
		lastWasInPartition = false;
	}
	
	@Override
	public Tuple getNextTuple() {
		//looping through while() but never returning
		while (Tr != null) {
			if (Ts == null) {
				Tr = R.getNextTuple();
				S.reset(sPartitionIndex);
				Ts = S.getNextTuple();
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
					return null;
				}
			}
			while (greater(Tr,Ts)) {
				lastWasInPartition = false;
				Ts = S.getNextTuple();
				sPartitionIndex++;
				if (Ts == null){
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
		return null;
	}
	
	private boolean lesser(Tuple tr, Tuple ts) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = ts.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int tsj = Integer.valueOf(ts.getValues().get(sAttrIndex));
			if (tri >= tsj)
				return false;
		}
		return true;
	}
	
	private boolean greater(Tuple tr, Tuple ts) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = ts.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int tsj = Integer.valueOf(ts.getValues().get(sAttrIndex));
			if (tri <= tsj)
				return false;
		}
		return true;
	}
	
	private boolean equal(Tuple tr, Tuple ts) {
		for (int i = 0; i < rSortCols.size(); i++) {
			int rAttrIndex = tr.getFields().indexOf(rSortCols.get(i));
			int sAttrIndex = ts.getFields().indexOf(sSortCols.get(i));
			int tri = Integer.valueOf(tr.getValues().get(rAttrIndex));
			int tsj = Integer.valueOf(ts.getValues().get(sAttrIndex));
			if (tri != tsj)
				return false;
		}
		return true;
	}

	@Override
	public void reset() {
		R.reset();
		S.reset();
		Tr = R.getNextTuple();
		Ts = S.getNextTuple();
		sPartitionIndex = 0;
		lastWasInPartition = false;
	}

}
