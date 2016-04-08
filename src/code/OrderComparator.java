package code;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The OrderComparator class defines a custom Comparator<Tuple> that will
 * be used to specify the different sort orders.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class OrderComparator implements Comparator<Tuple> {

	private ArrayList<String> columnPriority = new ArrayList<String>();
	
	public OrderComparator(ArrayList<String> sortColumns, ArrayList<String> allColumns) {
		//if there was no ORDER BY clause
		if (sortColumns == null)
			columnPriority.addAll(allColumns);
		else {
			ArrayList<String> tempFields = new ArrayList<String>();
			tempFields.addAll(allColumns);
			
			//remove duplicate columns in sortColumns
			Set<String> set = new LinkedHashSet<>(sortColumns);
			sortColumns.clear();
			sortColumns.addAll(set);
			//temporarily remove the columns mentioned in the ORDER BY clause from the list of all columns
			for (String sortCol : sortColumns) {
				int i = tempFields.indexOf(sortCol);
				tempFields.remove(i);
			}
		
			//create list of columns to be sorted on in the order they appear in the ORDER BY clause
			//followed by the rest of the columns that weren't specified
			columnPriority.addAll(sortColumns);
			columnPriority.addAll(tempFields);
		}
	}

	//overrides compare(T o1, T o2)
	public int compare(Tuple o1, Tuple o2) {
		if (o1 == null && o2 == null) return 0;
		else if (o1 == null) return 1;
		else if (o2 == null) return -1;
					
		ArrayList<String> fields = o1.getFields();
		
		for (String colname : columnPriority) {
			int index = fields.indexOf(colname);
			int v1 = Integer.parseInt(o1.getAttribute(index));
			int v2 = Integer.parseInt(o2.getAttribute(index));
			
			if (v1 < v2)
				return -1;
			else if (v1 > v2)
				return 1;
			//if v1 = v2, continue checking on the rest of the columns, in order they appear in columnPriority list
		}
		
		//if the two tuples are exactly the same, return 0
		return 0;
	}
}
