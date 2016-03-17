import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
/**
 * The Table class is instantiated whenever a ScanOperator
 * needs to "open" a certain data file. It stores the column
 * names and all of the data in the form of tuples.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class Table {

	private ArrayList<String> fields = new ArrayList<String>();
	private ArrayList<Tuple> tuples = new ArrayList<Tuple>();
	private int numTuples;
	
	public Table(String name, String alias) {
		DatabaseCatalog dbc = DatabaseCatalog.getInstance();
		Schema sch = dbc.getSchema(name);
		
		if (alias != null) {
			for (String c : sch.getCols()) {
				fields.add(alias + "." + c);
			}
		}
		else {
			for (String c : sch.getCols())
				fields.add(name + "." + c);
		}
		
		String loc = sch.getTableDir();
		readTuples(loc);
	}
	
	/**
	 * Helper method to read the data file, tuple by tuple,
	 * and adding each to the list
	 * 
	 * @param loc
	 */
	private void readTuples(String loc) {
		File datafile = new File(loc);

	    try {
	        Scanner sc = new Scanner(datafile);   
	        while (sc.hasNextLine()) {
	        	numTuples++;
	            String s = sc.nextLine();
	            tuples.add(new Tuple(s, fields));
	        }
	        sc.close();
	    } 
	    catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
	}
	
	/**
	 * Access method to return the arraylist of fields
	 * 
	 * @return fields
	 */
	public ArrayList<String> getFields() {
		return fields;
	}
	
	/**
	 * Access method to return the arraylist of Tuples.
	 * 
	 * @return tuples
	 */
	public ArrayList<Tuple> getTuples() {
		return tuples;
	}
	
	/**
	 * Access method to return the tuple at the 
	 * "ith row" of the table. Used in ScanOperator.
	 * 
	 * @param i
	 * @return tuple 
	 */
	public Tuple getTuple(int i) {
		if (i >= numTuples)
			return null;
		else
			return tuples.get(i);
	}
	
	/**
	 * Access method to return the number of tuples that
	 * are in the table
	 * 
	 * @return number of tuples
	 */
	public int getTableSize() {
		return numTuples;
	}
}
