package logical;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physical.*;

/**
 * PhysicalPlanBuilder uses the visitor pattern to traverse the logical plan 
 * generated by the LogicalPlanBuilder and build its own physical plan. 
 * It uses a stack of (physical) Operators based on each operator's children.
 * Like LogicalPlanBuilder, it is called in Parser and allows the Parser 
 * method access to the root so that dump can be called on it.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class PhysicalPlanBuilder {

	private Stack<Operator> ops;
	private String[] joinMethod;
	private String[] sortMethod;
	private boolean indexSelect;
	
	/**
	 * Initializes the PhysicalPlanBuilder by giving it the root of the logical plan
	 * and the directory of the config file that will tell it how to join/sort.
	 * 
	 * @param root
	 * @param configDir
	 */
	public PhysicalPlanBuilder(LogicalOperator root, String configDir) {
		File config = new File(configDir + File.separator + "plan_builder_config.txt");
		try {
	        Scanner sc = new Scanner(config);   
	        String joinMethod = sc.nextLine();
	        String sortMethod = sc.nextLine();
	        this.joinMethod = joinMethod.split(" ");
	        this.sortMethod = sortMethod.split(" ");
	        String indexSelectS = sc.nextLine();
	        if (indexSelectS.equals("0")) {
	        	indexSelect = false;
	        }
	        else {
	        	indexSelect = true;
	        }
	        sc.close();
	    } 
	    catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
		
		ops = new Stack<Operator>();
		root.accept(this);
	}
	
	/**
	 * Returns the root of the physical plan
	 * @return root Operator
	 */
	public Operator getRoot() {
		return ops.peek();
	}
	
	/**
	 * Visits a LogicalSelect operator; unary.
	 * @param logicalSelect
	 */
	public void visit(LogicalSelect logicalSelect) {
		if (indexSelect) {
			Expression e = logicalSelect.getCondition();
			// use visitor on condition e
				// will need : use Indexes to get columns a relation has index(es) for
			IndexExpressionVisitor visitor = new IndexExpressionVisitor(e);
			// part that is index-able is put into an IndexScan
			IndexScan scanOp = new IndexScan(null, null, null, false, 0, 0);
			// part that is not is put into a regular SelectOp with a Scan Op
			logicalSelect.getChild().accept(this);
			Operator child = ops.pop();
			SelectOperator newOp = new SelectOperator(child, null/* TODO non-indexable expr */);
			// TODO somehow join these??
		}
		else {
			logicalSelect.getChild().accept(this);
			Operator child = ops.pop();
			SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition());
			ops.push(newOp);
		}
	}

	/**
	 * Visits a LogicalProject operator; unary.
	 * @param logicalProject
	 */
	public void visit(LogicalProject logicalProject) {
		logicalProject.getChild().accept(this);
		Operator child = ops.pop();
		ProjectOperator newOp = new ProjectOperator(child, logicalProject.getSelectItems());
		ops.push(newOp);
	}
	
	/**
	 * Visits a LogicalJoin operator; binary.
	 * According to the config file, we decide what kind of
	 * join method will be used.
	 * @param logicalJoin
	 */
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
			String rightBaseTable = logicalJoin.getRightBaseTable();
			SortColumnExpressionVisitor visitor = new SortColumnExpressionVisitor(rightBaseTable);
			if (e != null)
				e.accept(visitor);
			// create two sorts as children (left and right)
			int sortType = Integer.valueOf(sortMethod[0]);
			Operator leftOp = null;
			Operator rightOp = null;
			switch(sortType) {
			case 0:
				leftOp = new SortOperator(left, visitor.getLeftSortCols());
				rightOp = new SortOperator(right, visitor.getRightSortCols());
				break;
			case 1:
				int numSortBuffers = Integer.valueOf(sortMethod[1]);
				leftOp = new ExternalSortOperator(left, numSortBuffers, visitor.getLeftSortCols());
				rightOp = new ExternalSortOperator(right, numSortBuffers, visitor.getRightSortCols());
				break;
			}
			// create SMJOperator with sorts as its children
			newOp = new SMJOperator(leftOp, rightOp, visitor.getLeftSortCols(), visitor.getRightSortCols());
			break;
		default:
			System.out.println("ERR: Join Type selection.");
		}
		ops.push(newOp);
	}

	/**
	 * Visits a LogicalSort operator; unary.
	 * According to the config file, we decide what kind of
	 * sort method will be used.
	 * @param logicalSort
	 */
	public void visit(LogicalSort logicalSort) {
		logicalSort.getChild().accept(this);
		Operator child = ops.pop();
		
		int sortType = Integer.valueOf(sortMethod[0]);
		Operator newOp = null;
		
		//ensure that implementation of DISTINCT doesn't use unbounded state (always uses P2's sorting method)
		if (logicalSort.getDistinct())
			sortType = 0;
		
		switch(sortType) {
		case 0:
			newOp = new SortOperator(child, logicalSort.getList());
			break;
		case 1:
			int numSortBuffers = Integer.valueOf(sortMethod[1]);
			List<OrderByElement> list = logicalSort.getList();
			ArrayList<String> sortList = new ArrayList<String>();
			for (OrderByElement o : list)
				sortList.add(o.getExpression().toString());
			newOp = new ExternalSortOperator(child, numSortBuffers, sortList);
			break;
		}
		ops.push(newOp);
	}

	/**
	 * Visits a LogicalDuplicateElimination operator; unary.
	 * @param logicalDuplicateElimination
	 */
	public void visit(LogicalDuplicateElimination logicalDuplicateElimination) {
		logicalDuplicateElimination.getChild().accept(this);
		Operator child = ops.pop();

		int sortType = Integer.valueOf(sortMethod[0]);
		Operator newOp = null;
		
		switch(sortType) {
		case 0:
			newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList(), 0);
			break;
		case 1:
			int numSortBuffers = Integer.valueOf(sortMethod[1]);
			newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList(), numSortBuffers);
			break;
		}
		ops.push(newOp);
	}

	/**
	 * Visits a LogicalScan operator; unary.
	 * @param logicalScan
	 */
	public void visit(LogicalScan logicalScan) {
		ScanOperator newOp = new ScanOperator(logicalScan.getTablename());
		ops.push(newOp);
	}

}
