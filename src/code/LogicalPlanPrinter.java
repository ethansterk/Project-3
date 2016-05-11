package code;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;
import logical.*;

public class LogicalPlanPrinter {

	private int nestLevel;
	
	public LogicalPlanPrinter(LogicalOperator root) {
		root.accept(this);
		nestLevel = 0;
	}

	public void visit(LogicalDuplicateElimination logicalDuplicateElimination) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		System.out.print("DupElim\n");
		nestLevel++;
		logicalDuplicateElimination.getChild().accept(this);
	}

	public void visit(LogicalJoin logicalJoin) {
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

	public void visit(LogicalProject logicalProject) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		List<SelectItem> projCols = logicalProject.getSelectItems();
		System.out.print("Project" + projCols.toString() + '\n');
		nestLevel++;
		logicalProject.getChild().accept(this);
	}

	public void visit(LogicalScan logicalScan) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		String baseTable = logicalScan.getTablename();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		System.out.print("Leaf[" + baseTable + "]\n");
	}

	public void visit(LogicalSelect logicalSelect) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		Expression e = logicalSelect.getCondition();
		System.out.print("Select[" + e.toString() + "]\n");
		nestLevel++;
		logicalSelect.getChild().accept(this);
	}

	public void visit(LogicalSort logicalSort) {
		for(int i = 0; i < nestLevel; i++)
			System.out.print("-");
		ArrayList<String> sortCols = logicalSort.getColumns();
		System.out.print("Sort" + sortCols + '\n');
		nestLevel++;
		logicalSort.getChild().accept(this);
	}

}
