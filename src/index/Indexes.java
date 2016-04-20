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
	private static ArrayList<Integer> keys;
	private static ArrayList<ArrayList<RecordID>> entries;
	private static ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>> children;
	private static ArrayList<DataEntry> allDataEntries;
	private static ArrayList<Node<Integer, ArrayList<RecordID>>> leaves;
	private static ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> anIndexLayer;
	private static ArrayList<ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>> allIndexLayers;
	
	public static Indexes getInstance() {
		return instance;
	}
	
	private Indexes() {
		keys = new ArrayList<Integer>();
		entries = new ArrayList<ArrayList<RecordID>>();
		children = new ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>>();
		allDataEntries = new ArrayList<DataEntry>();
		leaves = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
		anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
		allIndexLayers = new ArrayList<ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>>();
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
		File indexFile = new File(dir);		//don't delete!
		DatabaseCatalog dc = DatabaseCatalog.getInstance();
		Schema sch = dc.getSchema(tableName);
		String tableDir = sch.getTableDir();
		
		//if clustered, sort it first
		if (isClustered)
			sortClustered(tableName, sortCol, tableDir);

		//create the index using bulk-loading
		//0. generate data entries, in sorted order
		int sortAttIndex = sch.getCols().indexOf(attrName);
		HashMap<Integer, DataEntry> dataEntryMap = generateDataEntries(tableName, sortAttIndex);
		if (dataEntryMap == null)
			return;
		
		//sort the data entries, if unclustered (data entries are already in order if clustered)
		allDataEntries = new ArrayList<DataEntry>(dataEntryMap.values());
		if (!isClustered) {
			DataEntryComparator dec = new DataEntryComparator();
			Collections.sort(allDataEntries, dec);
		}
		
		//special case where there's only one LeafNode, create 2-node tree (1 leaf, 1 index)
		if (allDataEntries.size() <= 2 * D) {
			createTwoNodeTree();
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
			if (k <= (2 *D))				//aka, if numLeafNode = 1; just fill one LeafNode
				addKeysAndEntries(entryIndex, allDataEntries.size());
			else if (k >= (3 * D)) {		//fill last 2 LeafNodes as normal
				addKeysAndEntries(0, 2*D);								//second-to-last LeafNode, holding 2D entries
				addKeysAndEntries(entryIndex, allDataEntries.size());	//last LeafNode, holding remainder entries (at least D entries)
			}
			else {							//construct last 2 LeafNodes so that first gets k/2 entries and second gets remainder
				int divide = k/2;
				addKeysAndEntries(0, divide);							//second-to-last LeafNode, holding k/2 entries
				addKeysAndEntries(entryIndex, allDataEntries.size());	//last LeafNode, holding remainder entries
			}
			
			//2. create index node layers until have root (1 IndexNode), using ArrayList<LeafNode<...>> leaves
			anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
			allIndexLayers = new ArrayList<ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>>();
			m = leaves.size();
			//create first IndexNode layer and return the number in that layer to set up for the next
			m = createFirstIndexLayer(D);
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
				if (m <= (2 * D + 1))				//aka, if numIndexNode = 1; just fill one IndexNode
					addChildrenAndKeys(lastLayer, entryIndex, lastLayer.size());
				else if (m >= (3 * D + 2)) {		//fill last 2 IndexNodes as normal
					addChildrenAndKeys(lastLayer, 0, 2*D+1);						//second-to-last IndexNode, holding 2D + 1 children
					addChildrenAndKeys(lastLayer, entryIndex, lastLayer.size());	//last IndexNode, holding remainder children (at least D + 1 children)
				}
				else {						//construct last 2 IndexNodes so that first get
					int divide = m/2;
					addChildrenAndKeys(lastLayer, 0, divide);						//second-to-last IndexNode, holding m/2 children
					addChildrenAndKeys(lastLayer, entryIndex, lastLayer.size());	//last IndexNode, holding remainder children
				}
				
				allIndexLayers.add(anIndexLayer);
				//update size of latest layer (for checking if we're at the root (size=1))
				m = anIndexLayer.size();
			}
			//the top anIndexLayer holds just one IndexNode, which is the root
			root = anIndexLayer.get(0);
		}		
		
		//serialize the index into the pages that make up the index File @ dir
		TupleWriter tw = new TupleWriter(dir);
		//1 -- header page is first
			//contains:
			//a -- address of root
			//b -- number leaves
			//c -- order of tree (d)
		int totalIndexes = 0;
		for (ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> a : allIndexLayers)
			totalIndexes += a.size();
		int totalLeaves = leaves.size();
		int rootAddr = totalLeaves + totalIndexes;
		tw.writeOneInt(rootAddr);		//address of root
		tw.writeOneInt(totalLeaves);	//number of leaves
		tw.writeOneInt(D);				//order of tree
		tw.writeNewPageIndex();
		//2 -- serialize leaf nodes left-to-right (each to its own page)
		for (Node<Integer, ArrayList<RecordID>> ln : leaves) {
			ArrayList<Integer> keys = ((LeafNode<Integer, ArrayList<RecordID>>)ln).getKeys();
			ArrayList<ArrayList<RecordID>> data = ((LeafNode<Integer, ArrayList<RecordID>>)ln).getValues();
			tw.writeOneInt(0);					//flag for LeafNode
			tw.writeOneInt(keys.size());		//number of data entries in node
			//write serialized rep of each data entry in the node ln
			for (int i = 0; i < data.size(); i++) {
				tw.writeOneInt(keys.get(i));		//value of k
				ArrayList<RecordID> rec = data.get(i);
				tw.writeOneInt(rec.size());			//number of rids in entry
				tw.writeRecordIDs(rec);				//(p,t) for each rid
			}
			tw.writeNewPageIndex();
		}
		//3 -- go through allIndexLayers, each IndexNode in each layer (root is last page in file)
		int prevLayersCount = 0;
		for (ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> layer : allIndexLayers) {
			for (int i = 0; i < layer.size(); i++) {
				IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> index = layer.get(i);
				tw.writeOneInt(1);				//flag for IndexNode
				ArrayList<Integer> keys = index.getKeys();
				tw.writeOneInt(keys.size());	//number of keys in node
				tw.writeManyInts(keys);			//actual keys in node, in order
				ArrayList<Node<Integer, ArrayList<RecordID>>> children = index.getLeafChildren();
				if (i == 0) {		//if we're on the first IndexNode layer
					for (Node<Integer, ArrayList<RecordID>> child : children) {
						int addr = leaves.indexOf(child);
						tw.writeOneInt(addr);	//addresses of all children of node, in order
					}
				}
				else {				//if we're on any higher IndexNode layer
					for (Node<Integer, ArrayList<RecordID>> child : children) {
						int addr = leaves.size() + prevLayersCount + children.indexOf(child);
						tw.writeOneInt(addr);	//addresses of all children of node, in order
					}
				}	
				prevLayersCount += children.size();
				tw.writeNewPageIndex();
			}
		}
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
	 * Helper function that does the work of generating the data entries.
	 * @param tableName
	 * @param sortAttIndex
	 * @return dataEntryMap
	 */
	private static HashMap<Integer, DataEntry> generateDataEntries(String tableName, int sortAttIndex) {
		HashMap<Integer, DataEntry> dataEntryMap = new HashMap<Integer, DataEntry>();
		TupleReader tr = new TupleReader(tableName, null, false, null, null);
		Tuple t = tr.readNextTuple();
		if (t == null)
			return null;
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
		return dataEntryMap;
	}
	
	/**
	 * Helper function that does the work of generating a two-node index tree,
	 * such that only one LeafNode has entries, with one parent IndexNode.
	 */
	private static void createTwoNodeTree() {
		keys = new ArrayList<Integer>();
		entries = new ArrayList<ArrayList<RecordID>>();
		leaves = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
		for (DataEntry de : allDataEntries) {
			keys.add(de.getSortKey());
			entries.add(de.getRecIDs());
		}
		leaves.add(new LeafNode<Integer, ArrayList<RecordID>>(keys, entries));
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
		if (m <= (2 * D + 1))				//aka, if numIndexNode = 1; just fill one IndexNode
			addLeafChildrenAndKeys(entryIndex, leaves.size());
		else if (m >= (3 * D + 2)) {		//fill last 2 IndexNodes as normal
			addLeafChildrenAndKeys(0, 2*D+1);						//second-to-last IndexNode, holding 2D + 1 children
			addLeafChildrenAndKeys(entryIndex, leaves.size());		//last IndexNode, holding remainder children (at least D + 1 children)
		}
		else {						//construct last 2 IndexNodes so that first get
			int divide = m/2;
			addLeafChildrenAndKeys(0, divide);						//second-to-last IndexNode, holding m/2 children
			addLeafChildrenAndKeys(entryIndex, leaves.size());		//last IndexNode, holding remainder children
		}
		
		allIndexLayers.add(anIndexLayer);
		//update size of latest layer (for checking if we're at the root (size=1))
		return anIndexLayer.size();
	}

	/**
	 * Helper function to create a new IndexNode after adding the right children and keys,
	 * SPECIFICALLY for the first layer of IndexNodes above the LeafNode layer.
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
