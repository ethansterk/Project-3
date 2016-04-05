package code;
import java.io.*;
import java.util.HashMap;
import java.util.Scanner;

/**
 * The DatabaseCatalog keeps track of information such as:
 * 	- where a file for a given table is located
 *  - what the schema of different tables is
 *  - etc.
 * It acts as a global entity that various components of the system may want
 * to access, so it uses the singleton pattern.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 * 
 * see https://en.wikipedia.org/wiki/Singleton_pattern
 *
 */
public class DatabaseCatalog {
	
	private static final DatabaseCatalog instance = new DatabaseCatalog();
	private static final HashMap<String,Schema> schemas = new HashMap<String,Schema>();
	private static HashMap<String,String> tableFromAlias = new HashMap<String,String>();
	
	/**
	 * Since there is only one instance of the DBCatalog, there is no need
	 * for a public constructor.
	 */
	private DatabaseCatalog() {
		
	}
	
	/**
	 * Creates the DatabaseCatalog. This is called only once, from the
	 * main() function in Parser. It constructs the DBCatalog by
	 * creating the schema from the schema file.
	 * 
	 * @param inputDir
	 */
	public static void createCatalog(String inputDir) {
		File schema = new File(inputDir + File.separator + "db" + File.separator + "schema.txt");

	    try {
	        Scanner sc = new Scanner(schema);   
	        while (sc.hasNextLine()) {
	            String s = sc.nextLine();
	            Schema sch = new Schema(s, inputDir);
	            schemas.put(sch.getName(),sch);
	        }
	        sc.close();
	    } 
	    catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
	}
	
	/**
	 * Access method to return the one and only instance of DatabaseCatalog.
	 * 
	 * @return DatabaseCatalog
	 */
	public static DatabaseCatalog getInstance() {
		return instance;
	}
	
	/**
	 * Access method to look at the schemas, given the corresponding
	 * table's name
	 * 
	 * @param name
	 * @return Schema
	 */
	public Schema getSchema(String name) {
		return schemas.get(name);
	}
	
	public void addAlias(String alias, String table) {
		tableFromAlias.put(alias, table);
	}
	
	public String getTableFromAlias(String alias) {
		return tableFromAlias.get(alias);
	}
}
