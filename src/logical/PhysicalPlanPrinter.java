package logical;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import code.OutputWriter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;
import physical.*;

/**
 * PhysicalPlanPrinter uses the visitor pattern to print a human-readable
 * version of the physical plan.
 * @author Laura Ng (ln233) and Ethan Sterk (ejs334)
 *
 */
public class PhysicalPlanPrinter {

	private int nestLevel;
	private PrintStream output;
	
	public PhysicalPlanPrinter(Operator root, String outputDir) {
		nestLevel = 0;
		int queryNumber = OutputWriter.getInstance().getQueryNumber();
		File f = new File(outputDir + File.separator + "query" + queryNumber + "_physicalplan");
		try {
			output = new PrintStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		root.accept(this);
	}

	public void visit(DuplicateEliminationOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		output.print("DupElim\n");
		nestLevel++;
		op.getChild().accept(this);
	}
	
	public void visit(JoinOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		Expression unusable = op.getCondition();
		String unusableS;
		if (unusable == null)
			unusableS = "";
		else
			unusableS = "[" + unusable.toString() + "]";
		output.print("TNLJ" + unusableS + '\n');
		nestLevel++;
		op.getLeftChild().accept(this);
		op.getRightChild().accept(this);
	}

	public void visit(BNLJOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		Expression unusable = op.getCondition();
		String unusableS;
		if (unusable == null)
			unusableS = "";
		else
			unusableS = "[" + unusable.toString() + "]";
		output.print("BNLJ" + unusableS + '\n');
		nestLevel++;
		op.getLeftChild().accept(this);
		op.getRightChild().accept(this);
	}
	
	public void visit(SMJOperator op) {
		//TODO
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
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
		
		output.print("Join" + joinC + '\n');
		nestLevel++;
		op.getLeftChild().accept(this);
		op.getRightChild().accept(this);
	}

	public void visit(ProjectOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		List<SelectItem> projCols = op.getSelectItems();
		output.print("Project" + projCols.toString() + '\n');
		nestLevel++;
		op.getChild().accept(this);
	}

	public void visit(ScanOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		String baseTable = op.getTablename();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		output.print("TableScan[" + baseTable + "]\n");
	}

	public void visit(SelectOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		Expression e = op.getCondition();
		output.print("Select[" + e.toString() + "]\n");
		nestLevel++;
		op.getChild().accept(this);
	}

	public void visit(ExternalSortOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		ArrayList<String> sortCols = op.getColumns();
		output.print("ExternalSort" + sortCols + '\n');
		nestLevel++;
		op.getChild().accept(this);
	}

	public void visit(IndexScan op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		String baseTable = op.getTablename();
		String attribute = op.getAttribute();
		int lowkey = op.getLowKey();
		int highkey = op.getHighKey();
		String[] split = baseTable.split(" ");
		if (split.length > 1)
			baseTable = split[0];
		output.print("IndexScan[" + baseTable + "," + attribute + "," + lowkey + "," + highkey + "]\n");
	}

	public void visit(SortOperator op) {
		for(int i = 0; i < nestLevel; i++)
			output.print("-");
		ArrayList<String> sortCols = op.getColumns();
		output.print("InMemorySort" + sortCols + '\n');
		nestLevel++;
		op.getChild().accept(this);
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
