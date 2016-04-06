package physical;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
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
	}
	
	@Override
	public Tuple getNextTuple() {
		while (Tr != null && Gs != null) {
			while (lesser(Tr,Gs)) {
				Tr = R.getNextTuple();
				if (Tr == null) return null;
			}
			while (greater(Tr,Gs)) {
				Gs = S.getNextTuple();
				if (Gs == null) return null;
			}
			Ts = Gs;
			while (equal(Tr,Gs)) {
				Ts = Gs;
				while (equal(Ts,Tr)) {
					//add <Tr,Ts> to result (return)
					Ts = S.getNextTuple();
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
