package logical;
import index.Indexes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;

import code.DatabaseCatalog;
import code.Stats;
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
	//private String[] joinMethod;
	//private String[] sortMethod;
	//private boolean indexSelect;
	// TODO hard-coded for now:
	private int joinType = 2;
	private int joinBufferSize = 5;
	private int sortType = 1;
	private int sortBufferSize = 5;
	
	/**
	 * Initializes the PhysicalPlanBuilder by giving it the root of the logical plan
	 * and the directory of the config file that will tell it how to join/sort.
	 * 
	 * @param root
	 * @param configDir
	 */
	public PhysicalPlanBuilder(LogicalOperator root/*, String inputDir*/) {
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
		ArrayList<String> baseTables = logicalSelect.getChild().getBaseTables();
		if (baseTables.size() != 1) {
			System.out.println("ERR: getBaseTables wrong for selection");
		}
		String s = baseTables.get(0);
		String[] tokens = s.split(" ");
		String baseTable = tokens[0];
		
		Expression e = logicalSelect.getCondition();
		String leastCostIndex = calculateCosts(baseTable, e);
		
		if (leastCostIndex == null) { // normal scan is best cost
			logicalSelect.getChild().accept(this);
			Operator child = ops.pop();
			SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition());
			ops.push(newOp);
			return;
		}
		
		// use visitor on condition e
		IndexExpressionVisitor visitor = new IndexExpressionVisitor(e, leastCostIndex);
		e.accept(visitor);
		Expression indexE = visitor.getIndexCond();
		
		if (indexE != null) {
			int highKey = visitor.getHighKey();
			int lowKey = visitor.getLowKey();
			String indexDir = Indexes.getInstance().getIndexDir(baseTable + "." + leastCostIndex);
			boolean isClustered = Indexes.getInstance().getClustered(baseTable);
			// part that is index-able is put into an IndexScan
			IndexScan scanOp = new IndexScan(indexDir, s, isClustered, lowKey, highKey, leastCostIndex);
			// part that is non index is put into a regular SelectOp with a Scan Op
			Expression regE = visitor.getRegCond();
			Operator newOp = null;
			if(regE != null)
				newOp = new SelectOperator(scanOp, regE);
			else
				newOp = scanOp;
			ops.push(newOp);
			return;
		}
		else {
			logicalSelect.getChild().accept(this);
			Operator child = ops.pop();
			SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition());
			ops.push(newOp);
			return;
		}
	}

	/**
	 * This function calculates the cost of scanning the relation by
	 * using a normal scan versus using index scans.
	 * @param baseTable The table to be scanned. (In form "R","S","T", etc.).
	 * @param e The selection condition we are considering.
	 * @return The name of the column on which we are using the index for. If
	 * not using an index scan, return null.
	 */
	private String calculateCosts(String baseTable, Expression e) {
		// calculate cost of regular scan
		Stats stats = DatabaseCatalog.getInstance().getSchema(baseTable).getStats();
		int numTuples = stats.getNumTuples();
		int tupleSize = stats.getCols().size();
		int regularScanCost = numTuples * tupleSize / 4096;
		// calculate cost of each index
		String leastCostIndex = null; // if it stays null, normal scan is best cost
		int leastCost = regularScanCost;
		
		Indexes ind = Indexes.getInstance();
		ArrayList<String> indexCols = ind.getIndexCols(baseTable);
		for (String indexCol : indexCols) {
			IndexExpressionVisitor visitor = new IndexExpressionVisitor(e, indexCol);
			e.accept(visitor);
			int rangeSelection = visitor.getHighKey() - visitor.getLowKey();
			int rangeValues = stats.getColWithName(indexCol).getMaxVal() - stats.getColWithName(indexCol).getMinVal();
			int rf = 0;
			if (rangeValues != 0)
				rf = rangeSelection / rangeValues;
			int numPages = regularScanCost; // TODO is this correct?
			String indDir = ind.getIndexDir(baseTable + "." + indexCol);
			int numLeafPages = getNumLeafPages(indDir);
			
			boolean isClustered = ind.getClustered(baseTable);
			int indexCost = Integer.MAX_VALUE;
			if (isClustered) {
				indexCost = 3 + numPages * rf;
			}
			else {
				indexCost = 3 + numLeafPages * rf + numTuples * rf;
			}
			if (indexCost < leastCost) {
				leastCost = indexCost;
				leastCostIndex = indexCol;
			}
		}
		return leastCostIndex;
	}

	/**
	 * This function returns the number of leaf pages for a specific index.
	 * @param indexDir The directory of the index.
	 * @return The number of leaf pages of the index, based on the value
	 * stored on the header page of the index.
	 */
	@SuppressWarnings("resource")
	private int getNumLeafPages(String indexDir) {
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(indexDir);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		FileChannel fc = fin.getChannel();
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		try {
			if (fc.read(buffer) < 1) {
				System.out.println("ERR: reached end of FileChannel");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer.position(4);
		int numLeafPages = buffer.getInt();
		return numLeafPages;
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
		/*logicalJoin.getLeft().accept(this);
		logicalJoin.getRight().accept(this);
		Operator right = ops.pop();
		Operator left = ops.pop();*/
		
		// has arbitrary number of children -- must choose join order
		// have base tables R1, R2, ..., Rk
		// iterate over all subsets of R in increasing order of size
		int numChildren = logicalJoin.getChildren().size();
		//for (int subset = 1; subset <= numChildren; subset++) { // for each subset length
		//	for (int i = 0; i < numChildren; i++) { // for each combination of this subset
				
		//	}
		//}
		
		//dumb join logic (doesn't optimize join, uses only BNLJ-5pages) so that something works
		//how to split the condition?
		//first accept all children (in reverse order), so first child is at top of stack
		for (int i = numChildren - 1; i >= 0; i--) {
			logicalJoin.getChildren().get(i).accept(this);
		}
		//then chain them all together, left-deep style
		Operator first = ops.pop();
		Operator second = ops.pop();
		Operator temp = new BNLJOperator(first, second, logicalJoin.getCondition(), 5);
		for (int i = 2; i < numChildren; i++) {
			temp = new BNLJOperator(temp, ops.pop(), logicalJoin.getCondition(), 5); // TODO logicalJoin.getCondition() might not be correct
		}
		ops.push(temp);
		
		//UNTOUCHED CODE BELOW
//		Expression e = logicalJoin.getCondition();
//		Operator newOp = null;
//		switch(joinType) {
//		case 0:
//			newOp = new JoinOperator(left, right, e);
//			break;
//		case 1:
//			int bufferSize = joinBufferSize;
//			newOp = new BNLJOperator(left, right, e, bufferSize);
//			break;
//		case 2:
//			String rightBaseTable = logicalJoin.getRightBaseTable();
//			SortColumnExpressionVisitor visitor = new SortColumnExpressionVisitor(rightBaseTable);
//			if (e != null)
//				e.accept(visitor);
//			// create two sorts as children (left and right)
//			Operator leftOp = null;
//			Operator rightOp = null;
//			switch(sortType) {
//			case 0:
//				leftOp = new SortOperator(left, visitor.getLeftSortCols());
//				rightOp = new SortOperator(right, visitor.getRightSortCols());
//				break;
//			case 1:
//				int numSortBuffers = sortBufferSize;
//				leftOp = new ExternalSortOperator(left, numSortBuffers, visitor.getLeftSortCols());
//				rightOp = new ExternalSortOperator(right, numSortBuffers, visitor.getRightSortCols());
//				break;
//			}
//			// create SMJOperator with sorts as its children
//			newOp = new SMJOperator(leftOp, rightOp, visitor.getLeftSortCols(), visitor.getRightSortCols());
//			break;
//		default:
//			System.out.println("ERR: Join Type selection.");
//		}
//		ops.push(newOp);
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
		
		int tempSortType = sortType;
		Operator newOp = null;
		
		//ensure that implementation of DISTINCT doesn't use unbounded state (always uses P2's sorting method)
		if (logicalSort.getDistinct())
			tempSortType = 0;
		
		switch(tempSortType) {
		case 0:
			newOp = new SortOperator(child, logicalSort.getList());
			break;
		case 1:
			int numSortBuffers = sortBufferSize;
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

		int tempSortType = sortType;
		Operator newOp = null;
		
		switch(tempSortType) {
		case 0:
			newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList(), 0);
			break;
		case 1:
			int numSortBuffers = sortBufferSize;
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
