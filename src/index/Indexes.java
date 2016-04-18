package index;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

import code.DatabaseCatalog;
import code.Schema;
import code.Tuple;
import code.TupleReader;
import code.TupleWriter;
import physical.Operator;
import physical.ScanOperator;
import physical.SortOperator;

public class Indexes {

	private static final Indexes instance = new Indexes();
	private static final HashMap<String,String> indexDir = new HashMap<String,String>();
	
	public static Indexes getInstance() {
		return instance;
	}
	
	private Indexes() {
		
	}
	
	public static void createIndexes(String dbDir, boolean build) {
		File index_info = new File(dbDir + File.separator + "index_info.txt");
		Scanner sc = null;
		try {
			sc = new Scanner(index_info);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		while (sc.hasNextLine()) {
			String i = sc.nextLine();
			String[] tokens = i.split(" ");
			
			String key = tokens[0] + "." + tokens[1];
			String value = dbDir + File.separator + "indexes" + File.separator + key;
			
			indexDir.put(key, value);
			buildIndex(value, tokens);
		}
	}

	private static void buildIndex(String dir, String[] tokens) {
		String tableName = tokens[0];
		String attrName = tokens[1];
		ArrayList<String> sortCol = new ArrayList<String>();
		sortCol.add(attrName);
		File indexFile = new File(dir);
		boolean isClustered = tokens[2].equals("1");
		int order = Integer.parseInt(tokens[3]);
		DatabaseCatalog dc = DatabaseCatalog.getInstance();
		Schema sch = dc.getSchema(tableName);
		String tableDir = sch.getTableDir();
		
		//access the relation table we're creating the index from
		if (isClustered) {
			Operator op = new SortOperator(new ScanOperator(tableName), sortCol);
			
			//dump op into the source file using TupleWriter			
			File sourceFile = new File(tableDir);
			TupleWriter tw = new TupleWriter(tableDir);
			Tuple t = op.getNextTuple();
			//in the special case that there are no matching tuples whatsoever
			if (t == null) {
				return;
			}
			
			while (t != null) {
				tw.writeTuple(t);
				t = op.getNextTuple();
			}
			tw.writeNewPage();
		}
		
		//create the index using bulk-loading
		//0. generate data entries, in sorted order
		int sortAttIndex = sch.getCols().indexOf(attrName);
		TupleReader tr = new TupleReader(tableName, null, false, null, null);
		Tuple t = tr.readNextTuple();
		if (t == null)
			return;
		while (t != null) {
			HashMap<Integer, ArrayList<RecordID>> dataEntries = new HashMap<Integer, ArrayList<RecordID>>();
			String sortKey = t.getAttribute(sortAttIndex);
			if (dataEntries.containsKey(sortKey)) {
				ArrayList<RecordID> recIDs = dataEntries.get(sortKey);
				recIDs.add(new RecordID(tr.getCurrentPage(), tr.getCurrentTuple()));
			}
			else {
				
			}
		}
			
		//1. create leaf layer
		//2. create first index node layer
		//3. repeat 2 until we've reached the root (just one index node)
		
		
		
		//serialize the index into the pages that make up File f
		//1 -- header page is first
			//contains:
			//a -- address of root
			//b -- number leaves
			//c -- order of tree (d)
		//2 -- serialize leaf nodes left-to-right (each to its own page)
		//3 -- layer immediately above... so on (root is last page in file)
		
	}
	
	public String getIndexDir(String indexname) {
		if (indexDir.containsKey(indexname))
			return indexDir.get(indexname);
		else
			return null;
	}
}
