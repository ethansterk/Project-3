package index;

import java.util.Comparator;

public class DataEntryComparator implements Comparator<DataEntry> {

	public DataEntryComparator() {
	}
	
	//overrides compare(T o1, T o2)
	public int compare(DataEntry o1, DataEntry o2) {
		if (o1 == null && o2 == null) return 0;
		else if (o1 == null) return 1;
		else if (o2 == null) return -1;
		
		if (o1.getSortKey() < o2.getSortKey())
			return -1;
		else if (o1.getSortKey() > o2.getSortKey())
			return 1;
		else
			return 0;
	}

}
