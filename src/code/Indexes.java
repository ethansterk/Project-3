package code;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

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
		
		while (sc.hasNext()) {
			String i = sc.next();
			String[] tokens = i.split(" ");
			
			String key = tokens[0] + "." + tokens[1];
			String value = dbDir + File.separator + "indexes" + File.separator + key;
			
			indexDir.put(key, value);
			buildIndex(value, tokens);
		}
	}

	private static void buildIndex(String dir, String[] tokens) {
		String tableName = tokens[0];
		ArrayList<String> sortCol = new ArrayList<String>();
		sortCol.add(tokens[1]);
		File f = new File(dir);
		boolean isClustered = tokens[2].equals("1");
		int order = Integer.parseInt(tokens[3]);
		
		//access the relation table we're creating the index from
		Operator op = new ScanOperator(tableName);
		if (isClustered) {
			op = new SortOperator(op, sortCol);
			//dump op into the source file using TupleWriter
		}
		
		//create the index using bulk-loading
		
		
		
		
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
