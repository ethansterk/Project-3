import java.util.ArrayList;

/**
 * The Tuple class handles the Tuples from a data file as objects.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class Tuple {

	private ArrayList<String> values = new ArrayList<String>();
	private ArrayList<String> fields = new ArrayList<String>();
	
	/**
	 * Initializes the fields and their corresponding values.
	 * 
	 * @param s Contains the values for each of the fields, separated
	 * by commas.
	 * @param fields Contains the names of fields of this relation.
	 */
	public Tuple(String s, ArrayList<String> fields) {
		String[] temp = s.split(",");
		for (String token : temp) {
			values.add(token);
		}
		this.fields = fields;
	}
	
	/**
	 * Access method to return the tuple value corresponding
	 * to the ith column (determined by caller)
	 * 
	 * @param i
	 * @return value
	 */
	public String getAttribute(int i) {
		return values.get(i);
	}
	
	/**
	 * Access method to return the values (data)
	 * 
	 * @return values
	 */
	public ArrayList<String> getValues() {
		return values;
	}
	
	/**
	 * Access method to return the fields (column names)
	 * 
	 * @return fields
	 */
	public ArrayList<String> getFields() {
		return fields;
	}
	
	/**
	 * Helper function to convert the values back into 
	 * a string. For merging two tuples.
	 * 
	 * @return tuple as a string
	 */
	public String tupleString() {
		if (values.isEmpty()) return "";
		else {
			String temp = values.get(0);
			for (int i = 1; i < values.size(); i++) {
				temp += "," + values.get(i);
			}
			return temp;
		}
	}
	
	/**
	 * Return a new Tuple created from merging the Tuple one and
	 * Tuple two. Used in cross-product/join.
	 * 
	 * @param one
	 * @param two
	 * @return new Tuple
	 */
	public static Tuple merge(Tuple one, Tuple two) {
		String newTuple = one.tupleString() + "," + two.tupleString();
		ArrayList<String> newFields = new ArrayList<String>();
		newFields.addAll(one.getFields());
		newFields.addAll(two.getFields());
		return new Tuple(newTuple, newFields);
	}

	/**
	 * Method to see if tuples are the same
	 * 
	 * @param tempNext
	 * @return whether the tuples are the same
	 */
	public boolean sameAs(Tuple tempNext) {
		return (this.tupleString().equals(tempNext.tupleString()));
	}
}
