package logical;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;
import physical.*;

public class PhysicalPlanPrinter {

	private int nestLevel;
	
	public PhysicalPlanPrinter(Operator root) {
		root.accept(this);
		nestLevel = 0;
	}

	public void visit(DuplicateEliminationOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		System.out.print("DupElim\n");
		nestLevel++;
		op.getChild().accept(this);
	}
	
	public void visit(JoinOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression unusable = op.getCondition();
		String unusableS;
		if (unusable == null)
			unusableS = "";
		else
			unusableS = "[" + unusable.toString() + "]";
		System.out.print("TNLJ" + unusableS + '\n');
		nestLevel++;
		op.getLeftChild().accept(this);
		op.getRightChild().accept(this);
	}

	public void visit(BNLJOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression unusable = op.getCondition();
		String unusableS;
		if (unusable == null)
			unusableS = "";
		else
			unusableS = "[" + unusable.toString() + "]";
		System.out.print("BNLJ" + unusableS + '\n');
		nestLevel++;
		op.getLeftChild().accept(this);
		op.getRightChild().accept(this);
	}
	
	public void visit(SMJOperator op) {
		//TODO
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		String joinC = "";
		ArrayList<String> rCols = op.getRSortCols();
		ArrayList<String> sCols = op.getSSortCols();
		if (rCols.size() > 0)
			joinC = "[";
		for (int i = 0; i < rCols.size(); i++) {
			joinC += rCols.get(i) + " = " + sCols.get(i);
			if (i != rCols.size())
				joinC += " ";
			else
				joinC += "]";
		}
		
		System.out.print("Join" + joinC + '\n');
		nestLevel++;
		op.getLeftChild().accept(this);
		op.getRightChild().accept(this);
	}

	public void visit(ProjectOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		List<SelectItem> projCols = op.getSelectItems();
		System.out.print("Project" + projCols.toString() + '\n');
		nestLevel++;
		op.getChild().accept(this);
	}

	public void visit(ScanOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		String baseTable = op.getTablename();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		System.out.print("TableScan[" + baseTable + "]\n");
	}

	public void visit(SelectOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression e = op.getCondition();
		System.out.print("Select[" + e.toString() + "]\n");
		nestLevel++;
		op.getChild().accept(this);
	}

	public void visit(ExternalSortOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		ArrayList<String> sortCols = op.getColumns();
		System.out.print("ExternalSort" + sortCols + '\n');
		nestLevel++;
		op.getChild().accept(this);
	}

	public void visit(IndexScan op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		String baseTable = op.getTablename();
		String attribute = op.getAttribute();
		int lowkey = op.getLowKey();
		int highkey = op.getHighKey();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		System.out.print("IndexScan[" + baseTable + "," + attribute + "," + lowkey + "," + highkey + "]\n");
	}

	public void visit(SortOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		ArrayList<String> sortCols = op.getColumns();
		System.out.print("InMemorySort" + sortCols + '\n');
		nestLevel++;
		op.getChild().accept(this);
	}
}
