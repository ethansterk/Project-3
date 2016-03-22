package logical;
import java.io.File;
import java.util.Stack;

import physical.*;

public class PhysicalPlanBuilder {

	private Stack<Operator> ops;
	
	public PhysicalPlanBuilder(LogicalOperator root, File config) {
		ops = new Stack<Operator>();
		root.accept(this);
	}
	
	public Operator getRoot() {
		return ops.peek();
	}
	
	public void visit(LogicalSelect logicalSelect) {
		// TODO Auto-generated method stub
		logicalSelect.getChild().accept(this);
		Operator child = ops.pop();
		SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition());
		ops.push(newOp);
	}

	public void visit(LogicalProject logicalProject) {
		// TODO Auto-generated method stub
		logicalProject.getChild().accept(this);
		Operator child = ops.pop();
		ProjectOperator newOp = new ProjectOperator(child, logicalProject.getSelectItems());
		ops.push(newOp);
	}
	
	public void visit(LogicalJoin logicalJoin) {
		// TODO Auto-generated method stub
		logicalJoin.getLeft().accept(this);
		logicalJoin.getRight().accept(this);
		Operator right = ops.pop();
		Operator left = ops.pop();
		JoinOperator newOp = new JoinOperator(left, right, logicalJoin.getCondition());
		ops.push(newOp);
	}

	public void visit(LogicalSort logicalSort) {
		// TODO Auto-generated method stub
		logicalSort.getChild().accept(this);
		Operator child = ops.pop();
		SortOperator newOp = new SortOperator(child, logicalSort.getList());
		ops.push(newOp);
	}

	public void visit(LogicalDuplicateElimination logicalDuplicateElimination) {
		// TODO Auto-generated method stub
		logicalDuplicateElimination.getChild().accept(this);
		Operator child = ops.pop();
		DuplicateEliminationOperator newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList());
		ops.push(newOp);
	}

	public void visit(LogicalScan logicalScan) {
		// TODO Auto-generated method stub
		ScanOperator newOp = new ScanOperator(logicalScan.getTablename());
		ops.push(newOp);
	}

}
