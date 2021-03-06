package logical;
import index.Indexes;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import code.DatabaseCatalog;
import code.JoinEvaluateExpressionVisitor;
import code.MyUtils;
import code.Stats;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.Join;
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
			SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition(), baseTables);
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
			IndexScan scanOp = new IndexScan(indexDir, s, isClustered, lowKey, highKey, leastCostIndex, baseTables);
			// part that is non index is put into a regular SelectOp with a Scan Op
			Expression regE = visitor.getRegCond();
			Operator newOp = null;
			if(regE != null)
				newOp = new SelectOperator(scanOp, regE, baseTables);
			else
				newOp = scanOp;
			ops.push(newOp);
			return;
		}
		else {
			logicalSelect.getChild().accept(this);
			Operator child = ops.pop();
			SelectOperator newOp = new SelectOperator(child, logicalSelect.getCondition(), baseTables);
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
		ProjectOperator newOp = new ProjectOperator(child, logicalProject.getSelectItems(), logicalProject.getBaseTables());
		ops.push(newOp);
	}
	
	/**
	 * Visits a LogicalJoin operator; binary.
	 * According to the config file, we decide what kind of
	 * join method will be used.
	 * @param logicalJoin
	 */
	public void visit(LogicalJoin logicalJoin) {
		
		int numChildren = logicalJoin.getChildren().size();
		
		for (int i = numChildren - 1; i >= 0; i--) {
			logicalJoin.getChildren().get(i).accept(this);
		}
		//then chain them all together, left-deep style
		Expression unusable = logicalJoin.getUnionFind().getUnusable();
		
		// TODO iterate through union find elements, find conditions that
		// apply to each relation based on base table (Sailors AS S1)
		
		UnionFind uf = logicalJoin.getUnionFind();
		ArrayList<UnionFindElement> elements = uf.getElements();
		
		// keep track of which base tables we've used
		ArrayList<String> leftBaseTables = new ArrayList<String>();
		// start with first two tables -- check if element contains both in attributes
		Operator first = ops.pop();
		Operator second = ops.pop();
		String firstBT = first.getBaseTables().get(0);
		String secondBT = second.getBaseTables().get(0);
		String[] split = firstBT.split(" ");
		if (split.length > 1)
			firstBT = split[2];
		split = secondBT.split(" ");
		if (split.length > 1)
			secondBT = split[2];
		leftBaseTables.add(firstBT);
		leftBaseTables.add(secondBT);
		// keep track of attributes on left and right of join that are equal
		ArrayList<String> leftAtts = new ArrayList<String>();
		ArrayList<String> rightAtts = new ArrayList<String>();
		for (UnionFindElement element : elements) {
			ArrayList<String> atts = element.getAttributes();
			if (atts.size() < 2)
				continue;
			for (String att : atts) { // R.A, S.B, R.B
				String[] s = att.split("\\."); // [R,A]
				if (firstBT.equals(s[0])) {
					leftAtts.add(att);
				}
				else if (secondBT.equals(s[0])) {
					rightAtts.add(att);
				}
			}
		}
		Expression joinE = null;
		if (leftAtts.size() > 0 && rightAtts.size() > 0) {
			for (String rightAtt : rightAtts) {
				String stringE = rightAtt + "=" + leftAtts.get(0);
				Expression tempE = null;
				tempE = createExpressionFromString(tempE, stringE);
				joinE = MyUtils.safeConcatExpression(joinE, tempE);
			}
		}
		
		Operator temp;
		temp = new BNLJOperator(first, second, joinE, 5, null);
		for (int i = 2; i < numChildren; i++) {
			rightAtts.clear();
			Operator rightOp = ops.pop();
			String rightBT = rightOp.getBaseTables().get(0);
			String[] s = rightBT.split(" ");
			if (s.length > 1)
				rightBT = s[2];
			
			for (UnionFindElement element : elements) {
				ArrayList<String> atts = element.getAttributes();
				if (atts.size() < 2)
					continue;
				for (String att : atts) { // R.A, S.B, R.B
					String[] sp = att.split("\\."); // [R,A]
					if (leftBaseTables.contains(sp[0])) {
						leftAtts.add(att);
					}
					else if (rightBT.equals(sp[0])) {
						rightAtts.add(att);
					}
				}
			}
			joinE = null;
			if (leftAtts.size() > 0 && rightAtts.size() > 0) {
				for (String rightAtt : rightAtts) {
					String stringE = rightAtt + "=" + leftAtts.get(0);
					Expression tempE = null;
					tempE = createExpressionFromString(tempE, stringE);
					joinE = MyUtils.safeConcatExpression(joinE, tempE);
				}
			}
			if (i == numChildren)
				joinE = MyUtils.safeConcatExpression(joinE, unusable);
			temp = new BNLJOperator(temp, rightOp, joinE, 5, null);
			leftAtts.addAll(rightAtts);
			leftBaseTables.add(rightBT);
		}
		ops.push(temp);
	}

	private Expression createExpressionFromString(Expression tempE, String stringE) {
		CCJSqlParser parser = new CCJSqlParser(new StringReader(stringE));
		try {
			tempE = parser.Expression();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		return tempE;
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
			newOp = new SortOperator(child, logicalSort.getList(), logicalSort.getBaseTables());
			break;
		case 1:
			int numSortBuffers = sortBufferSize;
			List<OrderByElement> list = logicalSort.getList();
			ArrayList<String> sortList = new ArrayList<String>();
			for (OrderByElement o : list)
				sortList.add(o.getExpression().toString());
			newOp = new ExternalSortOperator(child, numSortBuffers, sortList, logicalSort.getBaseTables());
			break;
		}
		ops.push(newOp);
	}

	/**
	 * Visits a LogicalDuplicateElimination operator; unary.
	 * @param logicalDuplicateElimination
	 */
	public void visit(LogicalDuplicateElimination logicalDuplicateElimination) {
		ArrayList<String> baseTables = logicalDuplicateElimination.getBaseTables();
		logicalDuplicateElimination.getChild().accept(this);
		Operator child = ops.pop();

		int tempSortType = sortType;
		Operator newOp = null;
		
		switch(tempSortType) {
		case 0:
			newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList(), 0, baseTables);
			break;
		case 1:
			int numSortBuffers = sortBufferSize;
			newOp = new DuplicateEliminationOperator(child, logicalDuplicateElimination.getList(), numSortBuffers, baseTables);
			break;
		}
		ops.push(newOp);
	}

	/**
	 * Visits a LogicalScan operator; unary.
	 * @param logicalScan
	 */
	public void visit(LogicalScan logicalScan) {
		ScanOperator newOp = new ScanOperator(logicalScan.getTablename(), logicalScan.getBaseTables());
		ops.push(newOp);
	}

}
