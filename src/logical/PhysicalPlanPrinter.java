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
		Expression unusable = logicalJoin.getCondition();
		System.out.print("Join[" + unusable.toString() + "]\n");
		ArrayList<UnionFindElement> els = logicalJoin.getUnionFind().getElements();
		for (UnionFindElement el : els) {
			ArrayList<String> attrs = el.getAttributes();
			
			String eq;
			if (el.getEqualityConstr() == null)
				eq = "null";
			else
				eq = Integer.toString(el.getEqualityConstr());
			
			String min;
			if (el.getLowBound() == Integer.MAX_VALUE)
				min = "null";
			else
				min = Integer.toString(el.getLowBound());
			
			String max;
			if (el.getHighBound() == Integer.MIN_VALUE)
				max = "null";
			else
				max = Integer.toString(el.getHighBound());
			
			System.out.print("[" + attrs + ", equals " + eq + ", min " + min + ", max " + max + "]\n");
		}
		nestLevel++;
		ArrayList<LogicalOperator> children = logicalJoin.getChildren();
		for (LogicalOperator child : children) {
			child.accept(this);
		}
	}

	public void visit(BNLJOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression unusable = logicalJoin.getCondition();
		System.out.print("Join[" + unusable.toString() + "]\n");
		ArrayList<UnionFindElement> els = logicalJoin.getUnionFind().getElements();
		for (UnionFindElement el : els) {
			ArrayList<String> attrs = el.getAttributes();
			
			String eq;
			if (el.getEqualityConstr() == null)
				eq = "null";
			else
				eq = Integer.toString(el.getEqualityConstr());
			
			String min;
			if (el.getLowBound() == Integer.MAX_VALUE)
				min = "null";
			else
				min = Integer.toString(el.getLowBound());
			
			String max;
			if (el.getHighBound() == Integer.MIN_VALUE)
				max = "null";
			else
				max = Integer.toString(el.getHighBound());
			
			System.out.print("[" + attrs + ", equals " + eq + ", min " + min + ", max " + max + "]\n");
		}
		nestLevel++;
		ArrayList<LogicalOperator> children = logicalJoin.getChildren();
		for (LogicalOperator child : children) {
			child.accept(this);
		}
	}
	
	public void visit(SMJOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression unusable = logicalJoin.getCondition();
		System.out.print("Join[" + unusable.toString() + "]\n");
		ArrayList<UnionFindElement> els = logicalJoin.getUnionFind().getElements();
		for (UnionFindElement el : els) {
			ArrayList<String> attrs = el.getAttributes();
			
			String eq;
			if (el.getEqualityConstr() == null)
				eq = "null";
			else
				eq = Integer.toString(el.getEqualityConstr());
			
			String min;
			if (el.getLowBound() == Integer.MAX_VALUE)
				min = "null";
			else
				min = Integer.toString(el.getLowBound());
			
			String max;
			if (el.getHighBound() == Integer.MIN_VALUE)
				max = "null";
			else
				max = Integer.toString(el.getHighBound());
			
			System.out.print("[" + attrs + ", equals " + eq + ", min " + min + ", max " + max + "]\n");
		}
		nestLevel++;
		ArrayList<LogicalOperator> children = logicalJoin.getChildren();
		for (LogicalOperator child : children) {
			child.accept(this);
		}
	}

	public void visit(ProjectOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		List<SelectItem> projCols = logicalProject.getSelectItems();
		System.out.print("Project" + projCols.toString() + '\n');
		nestLevel++;
		logicalProject.getChild().accept(this);
	}

	public void visit(ScanOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		String baseTable = logicalScan.getTablename();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		System.out.print("Leaf[" + baseTable + "]\n");
	}

	public void visit(SelectOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression e = logicalSelect.getCondition();
		System.out.print("Select[" + e.toString() + "]\n");
		nestLevel++;
		logicalSelect.getChild().accept(this);
	}

	public void visit(SortOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		ArrayList<String> sortCols = logicalSort.getColumns();
		System.out.print("Sort" + sortCols + '\n');
		nestLevel++;
		logicalSort.getChild().accept(this);
	}

	public void visit(ExternalSortOperator op) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		ArrayList<String> sortCols = logicalSort.getColumns();
		System.out.print("Sort" + sortCols + '\n');
		nestLevel++;
		logicalSort.getChild().accept(this);
	}

	public void visit(IndexScan indexScan) {
		// TODO Auto-generated method stub
		
	}
}
