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
	private static final HashMap<String,String> relToIndex = new HashMap<String,String>();
	
	private static int entryIndex = 0;
	private static int k = 0;
	private static int m = 0;
	private static ArrayList<Integer> keys = null;
	private static ArrayList<ArrayList<RecordID>> entries = null;
	private static ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>> children = null;
	private static ArrayList<DataEntry> allDataEntries = null;
	private static ArrayList<Node<Integer, ArrayList<RecordID>>> leaves = null;
	private static ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> anIndexLayer = null;
	private static ArrayList<ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>> allIndexLayers = null;
	
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
			// TODO change for Project 5 when supports more than one column per relation
			relToIndex.put(key, tokens[1]);
			buildIndex(value, tokens);
		}
	}

	@SuppressWarnings("unchecked")
	private static <K extends Comparable<K>,T> void buildIndex(String dir, String[] tokens) {
		entryIndex = 0;
		k = 0;
		
		String tableName = tokens[0];
		String attrName = tokens[1];
		boolean isClustered = tokens[2].equals("1");
		int D = Integer.parseInt(tokens[3]);
		IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> root = null;
		
		ArrayList<String> sortCol = new ArrayList<String>();
		sortCol.add(tableName + "." + attrName);
		File indexFile = new File(dir);
		DatabaseCatalog dc = DatabaseCatalog.getInstance();
		Schema sch = dc.getSchema(tableName);
		String tableDir = sch.getTableDir();
		
		//if clustered, sort it first
		if (isClustered)
			sortClustered(tableName, sortCol, tableDir);

		//create the index using bulk-loading
		//0. generate data entries, in sorted order
		int sortAttIndex = sch.getCols().indexOf(attrName);
		HashMap<Integer, DataEntry> dataEntryMap = new HashMap<Integer, DataEntry>();
		TupleReader tr = new TupleReader(tableName, null, false, null, null);
		Tuple t = tr.readNextTuple();
		if (t == null)
			return;
		//read in all of the tuples
		while (t != null) {
			int sortKey = Integer.parseInt(t.getAttribute(sortAttIndex));
			if (dataEntryMap.containsKey(sortKey)) {
				DataEntry d = dataEntryMap.get(sortKey);
				d.insertRecordID(tr.getCurrentPage(), tr.getCurrentTuple());
			}
			else {
				ArrayList<RecordID> newRecIDs = new ArrayList<RecordID>();
				newRecIDs.add(new RecordID(tr.getCurrentPage(), tr.getCurrentTuple()));
				dataEntryMap.put(sortKey, new DataEntry(sortKey, newRecIDs));
			}
			t = tr.readNextTuple();
		}
		//sort the data entries, if unclustered (data entries are already in order if clustered)
		allDataEntries = new ArrayList<DataEntry>(dataEntryMap.values());
		if (!isClustered) {
			DataEntryComparator dec = new DataEntryComparator();
			Collections.sort(allDataEntries, dec);
		}
		
		//1&2. special case where there's only one LeafNode, create 2-node tree (1 leaf, 1 index)
		if (allDataEntries.size() <= 2 * D) {
			keys = new ArrayList<Integer>();
			entries = new ArrayList<ArrayList<RecordID>>();
			leaves = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
			for (DataEntry de : allDataEntries) {
				keys.add(de.getSortKey());
				entries.add(de.getRecIDs());
			}
			leaves.add(new LeafNode<Integer, ArrayList<RecordID>>(keys, entries));
			root = new IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>();
			root.setLeafChildren(leaves);
		}
		
		//else, create leaf layer then index layers following steps in instructions
		else {
			//1. create leaf layer
			entryIndex = 0;
			k = allDataEntries.size();		//number of entries left
			leaves = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
			int numLeafNodes = k / (2 * D);
			if (k % (2 * D) != 0)
				numLeafNodes++;
			//fill all LeafNodes normally except last 2
			while (numLeafNodes > 2) {
				addKeysAndEntries(0, 2*D);
				numLeafNodes--;
			}
			if (k <= (2 *D)) {				//aka, numLeafNode = 1; just fill one LeafNode
				addKeysAndEntries(entryIndex, allDataEntries.size());
			}
			else if (k >= (3 * D)) {		//fill last 2 LeafNodes as normal
				//second-to-last LeafNode, holding 2D entries
				addKeysAndEntries(0, 2*D);
				//last LeafNode, holding remainder entries (at least D entries)
				addKeysAndEntries(entryIndex, allDataEntries.size());
			}
			else {					//construct last 2 LeafNodes so that first gets k/2 entries and second gets remainder
				int divide = k/2;
				//second-to-last LeafNode, holding k/2 entries
				addKeysAndEntries(0, divide);
				//last LeafNode, holding remainder entries
				addKeysAndEntries(entryIndex, allDataEntries.size());
			}
			
			//2. create index node layers until have root (1 IndexNode), using ArrayList<LeafNode<...>> leaves
			anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
			allIndexLayers = new ArrayList<ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>>();
			m = leaves.size();
			m = createFirstIndexLayer(D);
			System.out.println("indexlayer " + allIndexLayers.size());
			for (IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> ind : anIndexLayer)
				System.out.println(ind.getKeys().toString());
			while (m > 1) {
				entryIndex = 0;
				ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>> lastLayer = new ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>>();
				lastLayer.addAll(anIndexLayer);
				anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
				
				int numIndexNodes = m / (2 * D + 1);
				if (m % (2 * D + 1) != 0)
					numIndexNodes++;
				//fill all IndexNodes normally except last 2
				while (numIndexNodes > 2) {
					addChildrenAndKeys(lastLayer, 0, 2*D+1);
					numIndexNodes--;
				}
				//System.out.println(entryIndex);
				if (m <= (2 * D + 1)) {				//aka, numIndexNode = 1; just fill one IndexNode
					addChildrenAndKeys(lastLayer, entryIndex, lastLayer.size());
				}
				else if (m >= (3 * D + 2)) {		//fill last 2 IndexNodes as normal
					//second-to-last IndexNode, holding 2D + 1 children
					addChildrenAndKeys(lastLayer, 0, 2*D+1);
					//last IndexNode, holding remainder children (at least D + 1 children)
					addChildrenAndKeys(lastLayer, entryIndex, lastLayer.size());
				}
				else {						//construct last 2 IndexNodes so that first get
					int divide = m/2;
					//second-to-last IndexNode, holding m/2 children
					addChildrenAndKeys(lastLayer, 0, divide);
					//last IndexNode, holding remainder children
					addChildrenAndKeys(lastLayer, entryIndex, lastLayer.size());
				}
				
				allIndexLayers.add(anIndexLayer);
				//update size of latest layer (for checking if we're at the root (size=1))
				m = anIndexLayer.size();
				System.out.println(m);
				System.out.println("indexlayer " + allIndexLayers.size() + ": " + anIndexLayer.get(0).getKeys().toString());
			}
			//the top anIndexLayer holds just one IndexNode, which is the root
			root = anIndexLayer.get(0);
			System.out.println("root's first key: " + root.getKeys().toString());
		}
		
		
		
		//serialize the index into the pages that make up File f
		//1 -- header page is first
			//contains:
			//a -- address of root
			//b -- number leaves
			//c -- order of tree (d)
		//2 -- serialize leaf nodes left-to-right (each to its own page)
		//3 -- layer immediately above... so on (root is last page in file)
		
	}
	
	/**
	 * Helper function to create the first layer of IndexNodes from the layer of LeafNodes.
	 * @param D
	 * @return new m (number of IndexNodes in this first layer)
	 */
	private static int createFirstIndexLayer(int D) {
		entryIndex = 0;
		anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
		
		int numIndexNodes = m / (2 * D + 1);
		if (m % (2 * D + 1) != 0)
			numIndexNodes++;
		//fill all IndexNodes normally except last 2
		while (numIndexNodes > 2) {
			addLeafChildrenAndKeys(0, 2*D+1);
			numIndexNodes--;
		}
		//System.out.println(entryIndex);
		if (m <= (2 * D + 1)) {				//aka, numIndexNode = 1; just fill one IndexNode
			addLeafChildrenAndKeys(entryIndex, leaves.size());
		}
		else if (m >= (3 * D + 2)) {		//fill last 2 IndexNodes as normal
			//second-to-last IndexNode, holding 2D + 1 children
			addLeafChildrenAndKeys(0, 2*D+1);
			//last IndexNode, holding remainder children (at least D + 1 children)
			addLeafChildrenAndKeys(entryIndex, leaves.size());
		}
		else {						//construct last 2 IndexNodes so that first get
			int divide = m/2;
			//second-to-last IndexNode, holding m/2 children
			addLeafChildrenAndKeys(0, divide);
			//last IndexNode, holding remainder children
			addLeafChildrenAndKeys(entryIndex, leaves.size());
		}
		
		allIndexLayers.add(anIndexLayer);
		//update size of latest layer (for checking if we're at the root (size=1))
		return anIndexLayer.size();
	}

	/**
	 * Helper function to create a new IndexNode after adding the right children and keys.
	 * @param i
	 * @param j
	 */
	private static void addLeafChildrenAndKeys(int start, int end) {
		keys = new ArrayList<Integer>();
		ArrayList<Node<Integer, ArrayList<RecordID>>> leafchildren = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
		//add first child, so that #children = m + 1 and #keys = m
		leafchildren.add(leaves.get(entryIndex));
		entryIndex++;
		m--;
		for (int i = start + 1; i < end; i++) {
			keys.add(leaves.get(entryIndex).getFirstKey());
			leafchildren.add(leaves.get(entryIndex));
			entryIndex++;
			m--;
		}
		IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> in = new IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>();
		in.setKeys(keys);
		in.setLeafChildren(leafchildren);
		anIndexLayer.add(in);
	}
	
	/**
	 * Helper function to create a new IndexNode after adding the right children and keys.
	 * @param i
	 * @param j
	 */
	private static void addChildrenAndKeys(ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>> lastLayer, int start, int end) {
		keys = new ArrayList<Integer>();
		children = new ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>>();
		//add first child, so that #children = m + 1 and #keys = m
		children.add(lastLayer.get(entryIndex));
		entryIndex++;
		m--;
		for (int i = start + 1; i < end; i++) {
			keys.add(lastLayer.get(entryIndex).getFirstKey());
			children.add(lastLayer.get(entryIndex));
			entryIndex++;
			m--;
		}
		IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> in = new IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>();
		in.setKeys(keys);
		in.setIndexChildren(children);
		anIndexLayer.add(in);
	}

	/**
	 * Helper function to create a new LeafNode after adding the right keys and entries; add to leaves (leaf layer).
	 * @param start
	 * @param end
	 */
	private static void addKeysAndEntries(int start, int end) {
		keys = new ArrayList<Integer>();
		entries = new ArrayList<ArrayList<RecordID>>();
		for (int i = start; i < end; i++) {
			keys.add(allDataEntries.get(entryIndex).getSortKey());
			entries.add(allDataEntries.get(entryIndex).getRecIDs());
			entryIndex++;
			k--;
		}
		leaves.add(new LeafNode<Integer, ArrayList<RecordID>>(keys, entries));
	}

	/**
	 * Helper function for building a clustered index.
	 * This function sorts the relation file and rewrites it to disk.
	 * @param tableName
	 * @param sortCol
	 * @param tableDir
	 */
	private static void sortClustered(String tableName, ArrayList<String> sortCol, String tableDir){
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
	
	/**
	 * Getter method for index directory corresponding with the given indexname.
	 * @param indexname
	 * @return
	 */
	public String getIndexDir(String indexname) {
		if (indexDir.containsKey(indexname))
			return indexDir.get(indexname);
		else
			return null;
	}
	
	/**
	 * Getter method for the index's key columns for a given relation.
	 * @param relation
	 * @return
	 */
	public String getIndexCols(String relation) {
		if (relToIndex.containsKey(relation))
			return relToIndex.get(relation);
		else
			return null;
	}
}
