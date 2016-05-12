package code;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;
import logical.*;

/**
 * LogicalPlanPrinter uses the visitor pattern to print a human-readable
 * version of the logical plan.
 * @author Laura Ng (ln233) and Ethan Sterk (ejs334)
 *
 */
public class LogicalPlanPrinter {

	private int nestLevel;
	private PrintStream output;
	
	public LogicalPlanPrinter(LogicalOperator root, String outputDir) {
		nestLevel = 0;
		int queryNumber = OutputWriter.getInstance().getQueryNumber();
		File f = new File(outputDir + File.separator + "query" + queryNumber + "_logicalplan");
		try {
			output = new PrintStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		root.accept(this);
	}

	public void visit(LogicalDuplicateElimination logicalDuplicateElimination) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		output.print("DupElim\n");
		nestLevel++;
		logicalDuplicateElimination.getChild().accept(this);
	}

	public void visit(LogicalJoin logicalJoin) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		Expression unusable = logicalJoin.getCondition();
		String unusableS;
		if (unusable == null)
			unusableS = "";
		else
			unusableS = "[" + unusable.toString() + "]";
		output.print("Join" + unusableS + '\n');
		ArrayList<UnionFindElement> els = logicalJoin.getUnionFind().getElements();
		for (UnionFindElement el : els) {
			ArrayList<String> attrs = el.getAttributes();
			
			String eq;
			if (el.getEqualityConstr() == null)
				eq = "null";
			else
				eq = Integer.toString(el.getEqualityConstr());
			
			String min;
			if (el.getLowBound() == null)
				min = "null";
			else
				min = Integer.toString(el.getLowBound());
			
			String max;
			if (el.getHighBound() == null)
				max = "null";
			else
				max = Integer.toString(el.getHighBound());
			
			if (eq.equals("null") && min.equals("null") && max.equals("null") && attrs.size() == 1)
				continue;
			output.print("[" + attrs + ", equals " + eq + ", min " + min + ", max " + max + "]\n");
		}
		nestLevel++;
		int temp = nestLevel;
		ArrayList<LogicalOperator> children = logicalJoin.getChildren();
		for (LogicalOperator child : children) {
			child.accept(this);
			nestLevel = temp;
		}
	}

	public void visit(LogicalProject logicalProject) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		List<SelectItem> projCols = logicalProject.getSelectItems();
		output.print("Project" + projCols.toString() + '\n');
		nestLevel++;
		logicalProject.getChild().accept(this);
	}

	public void visit(LogicalScan logicalScan) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		String baseTable = logicalScan.getTablename();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		output.print("Leaf[" + baseTable + "]\n");
	}

	public void visit(LogicalSelect logicalSelect) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		Expression e = logicalSelect.getCondition();
		output.print("Select[" + e.toString() + "]\n");
		nestLevel++;
		logicalSelect.getChild().accept(this);
	}

	public void visit(LogicalSort logicalSort) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		ArrayList<String> sortCols = logicalSort.getColumns();
		output.print("Sort" + sortCols + '\n');
		nestLevel++;
		logicalSort.getChild().accept(this);
	}

	/**
	 * Just as it describes, the method which flushes and closes
	 * the PrintStream output
	 */
	public void flushAndClose() {
		output.flush();
		output.close();
	}
}
