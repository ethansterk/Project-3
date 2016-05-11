package code;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Scanner;

import index.Indexes;
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
		String configDir = args[0];
		File config = new File(configDir + File.separator + "interpreter_config_file.txt");
		String inputDir = null;	
		String outputDir = null;
		String tempDir = null;
		String loggerDir = null;
		try {
	        Scanner sc = new Scanner(config);   
	        inputDir = sc.nextLine();
	        outputDir = sc.nextLine();
	        tempDir = sc.nextLine();
	        sc.close();
	    } 
	    catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
		
		DatabaseCatalog.createCatalog(inputDir);
		OutputWriter.createStream(outputDir);
		OutputWriter.createTempStream(tempDir);
		Logger.createLogger(loggerDir);
		String dbDir = inputDir + File.separator + "db";
		Indexes.createIndexes(dbDir);
		
		try {
			cleanTempDir(tempDir);
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
					
					LogicalPlanBuilder builderL = new LogicalPlanBuilder(fromItem, where, selectItems, joins, orderByElements, distinct);
					//produces logical tree with root
					// TODO From OH: use visitor to visitor builders and print
					LogicalOperator logRoot = builderL.getRoot();
					LogicalPlanPrinter printP = new LogicalPlanPrinter(logRoot);
					// prints the logical plan
					PhysicalPlanBuilder builderP = new PhysicalPlanBuilder(logRoot/*, inputDir*/);
					//produces physical tree with root
					Operator phiRoot = builderP.getRoot();
					//get time before dump
					long timeBefore = System.currentTimeMillis();
					phiRoot.dump();
					long timeAfter = System.currentTimeMillis();
					long elapsedTime = timeAfter - timeBefore;
					int queryNum = OutputWriter.getInstance().getQueryNumber();
					System.out.println("Time to run query" + queryNum + " = " + elapsedTime + " ms");
					
				} catch (Exception e) {
					System.err.println("Exception occurred while returning tuples");
					e.printStackTrace();
				}
				
				cleanTempDir(tempDir);
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
	//}
	
	/**
	 * Helper function: wipes the tempDir clean of any files
	 */
	private static void cleanTempDir(String tempDir) {
		File temp = new File(tempDir);
		String[] files = temp.list();
		for (String s : files) {
			File f = new File(tempDir + File.separator + s);
			if (f.exists()) f.delete();
		}
	}

}
