package logical;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import code.Schema;
import physical.*;

public class PhysicalPlanBuilder {

	private Stack<Operator> ops;
	private String[] joinMethod;
	private String[] sortMethod;
	
	public PhysicalPlanBuilder(LogicalOperator root, String configDir) {
		File config = new File(configDir + File.separator + "config.txt");
		try {
	        Scanner sc = new Scanner(config);   
	        String joinMethod = sc.nextLine();
	        String sortMethod = sc.nextLine();
	        this.joinMethod = joinMethod.split(" ");
	        this.sortMethod = sortMethod.split(" ");
	        sc.close();
	    } 
	    catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
		
		ops = new Stack<Operator>();
		root.accept(this);
	}
	
	public Operator getRoot() {
		return ops.peek();
	}
	
	public void visit(LogicalSelect logicalSelect) {
		logicalSelect.getChild().accept(this);
		Operator child = ops.pop();
		SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition());
		ops.push(newOp);
	}

	public void visit(LogicalProject logicalProject) {
		logicalProject.getChild().accept(this);
		Operator child = ops.pop();
		ProjectOperator newOp = new ProjectOperator(child, logicalProject.getSelectItems());
		ops.push(newOp);
	}
	
	public void visit(LogicalJoin logicalJoin) {
		logicalJoin.getLeft().accept(this);
		logicalJoin.getRight().accept(this);
		Operator right = ops.pop();
		Operator left = ops.pop();
		
		int joinType = Integer.valueOf(joinMethod[0]);
		Expression e = logicalJoin.getCondition();
		Operator newOp = null;
		switch(joinType) {
		case 0:
			newOp = new JoinOperator(left, right, e);
			break;
		case 1:
			int bufferSize = Integer.valueOf(joinMethod[1]);
			newOp = new BNLJOperator(left, right, e, bufferSize);
			break;
		case 2:
			// TODO add SMJ
			break;
		default:
			System.out.println("ERR: Join Type selection.");
		}
		ops.push(newOp);
	}

	public void visit(LogicalSort logicalSort) {
		logicalSort.getChild().accept(this);
		Operator child = ops.pop();
		// TODO use config settings here
		SortOperator newOp = new SortOperator(child, logicalSort.getList());
		ops.push(newOp);
	}

	public void visit(LogicalDuplicateElimination logicalDuplicateElimination) {
		logicalDuplicateElimination.getChild().accept(this);
		Operator child = ops.pop();
		DuplicateEliminationOperator newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList());
		ops.push(newOp);
	}

	public void visit(LogicalScan logicalScan) {
		ScanOperator newOp = new ScanOperator(logicalScan.getTablename());
		ops.push(newOp);
	}

}
