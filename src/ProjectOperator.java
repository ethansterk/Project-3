import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
/**
 * ProjectOperator is a representation of the relational algebra operator
 * project. The output of a project contains all the tuples contained in
 * the input, minus information contained in columns that do not appear
 * in the selectItems field. If the DISTINCT keyword in used in the SQL
 * query, duplicates are eliminated afterwards.
 * 
 * @author Ethan Sterk (ejs334), Laura Ng (ln233)
 *
 */
public class ProjectOperator extends Operator {
	
	private Operator child;
	private List<SelectItem> selectItems;
	
	/**
	 * Initializes a project operator with specific columns to select.
	 * 
	 * @param child The input for the projection.
	 * @param selectItems The columns to include in the output tuples.
	 */
	public ProjectOperator(Operator child, List<SelectItem> selectItems) {
		this.child = child;				//can be SelectOperator (if there's a WHERE clause) or a ScanOperator
		this.selectItems = selectItems;
	}

	/**
	 * Returns the next tuple as an output of the projection. It does
	 * this by retrieving a tuple as input, looping through each of
	 * the selectItems (the columns we want to keep), and storing info.
	 * from these columns in a new Tuple t. We return t as the output
	 * tuple.
	 * 
	 * @return t, the next tuple with projected columns
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t = child.getNextTuple();
		if (t != null) {
			ArrayList<String> tempFields = t.getFields();
			
			String newTuple = "";
			ArrayList<String> newFields = new ArrayList<String>();
			
			for (SelectItem si : selectItems) {
				//String alias = si.getAlias(); //column's alias = "sid" in "SELECT S.A AS sid"
				
				//For this, every SelectExpressionItem is a Column
				Column expr = (Column)((SelectExpressionItem)si).getExpression();
				String colname = expr.getWholeColumnName();
				
				int i = tempFields.indexOf(colname);
				newTuple += "," + t.getAttribute(i);
				newFields.add(tempFields.get(i));
			}
			t = new Tuple(newTuple.substring(1), newFields);
		}
		return t;
	}

	@Override
	public void reset() {
		child.reset();
	}
}
