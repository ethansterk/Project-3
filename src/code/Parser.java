package code;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import logical.LogicalOperator;
import logical.PhysicalPlanBuilder;
import physical.Operator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
/**
 * Initializes the Database Catalog. Then, reads SQL queries from file;
 * parses the SQL queries using CCJsqlParser library; creates appropriate
 * operators; uses operators to evaluate each query.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class Parser {
	
	/**
	 * The evaluation of the SQL query(ies) begins here. The directory
	 * of the input folder must be passed as the 0th system argument.
	 * The function then proceeds to evaluate every query contained in
	 * the queries.sql file, writing the results of the queries to 
	 * appropriate files.
	 * 
	 * @param args Contains the directory of /input/ as the 0th arg.
	 */
	public static void main(String[] args) {
		String inputDir = args[0];	
		String outputDir = args[1];
		String tempDir = args[2];
		String loggerDir = args[2];
		DatabaseCatalog.createCatalog(inputDir);
		OutputWriter.createStream(outputDir);
		OutputWriter.createTempStream(tempDir);
		Logger.createLogger(loggerDir);
		
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(inputDir + File.separator + "queries.sql"));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				OutputWriter.getInstance().increment();   //increments the query number
				
				try {
					
					Select select = (Select) statement;
					PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
					FromItem fromItem = plainSelect.getFromItem();
					Expression where = plainSelect.getWhere();
					@SuppressWarnings("unchecked")
					List<SelectItem> selectItems = plainSelect.getSelectItems();
					@SuppressWarnings("unchecked")
					List<Join> joins = plainSelect.getJoins();
					@SuppressWarnings("unchecked")
					List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
					boolean distinct = (plainSelect.getDistinct() != null);
					
					//Changes for P3 here:
					LogicalPlanBuilder builderL = new LogicalPlanBuilder(fromItem, where, selectItems, joins, orderByElements, distinct);
					//produces logical tree with root
					LogicalOperator logRoot = builderL.getRoot();
					PhysicalPlanBuilder builderP = new PhysicalPlanBuilder(logRoot, inputDir);
					//produces physical tree with root
					Operator phiRoot = builderP.getRoot();
					//get time before dump
					long timeBefore = System.currentTimeMillis();
					//dump physical root
					phiRoot.dump();
					long timeAfter = System.currentTimeMillis();
					long elapsedTime = timeAfter - timeBefore;
					int queryNum = OutputWriter.getInstance().getQueryNumber();
					//Logger.log("Time to run query" + queryNum + " = " + elapsedTime + " ms");
					System.out.println("Time to run query" + queryNum + " = " + elapsedTime + " ms");
					
					
					
				} catch (Exception e) {
					System.err.println("Exception occurred while returning tuples");
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

}
