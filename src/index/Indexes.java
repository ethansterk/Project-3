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

/**
 * The Indexes class is used to build each Index that is listed
 * in "index_info.txt". It uses a BPlusTree structure adapted from
 * CS4320 HW2, but it's static (doesn't support inserting or deleting).
 * This class relies on every other class in the index package.
 * 
 * @author Ethan Sterk (ejs334) and Laura Ng (ln233)
 *
 */
public class Indexes {

	private static final Indexes instance = new Indexes();
	private static final HashMap<String,String> indexDir = new HashMap<String,String>();
	private static final HashMap<String,ArrayList<String>> relToIndex = new HashMap<String,ArrayList<String>>();
	private static final HashMap<String,Boolean> relToClustered = new HashMap<String,Boolean>();
	
	private static int entryIndex = 0;
	private static int k = 0;
	private static int m = 0;
	private static int currentAddr = 1;
	private static ArrayList<DataEntry> allDataEntries;
	private static ArrayList<Node<Integer, ArrayList<RecordID>>> leaves;
	private static ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> anIndexLayer;
	
	public static Indexes getInstance() {
		return instance;
	}
	
	private Indexes() {
		allDataEntries = new ArrayList<DataEntry>();
		leaves = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
		anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
	}
	
	/**
	 * This function can be accessed publicly to initiate the construction
	 * of all the necessary indexes.
	 * @param dbDir
	 */
	public static void createIndexes(String dbDir) {
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
			String tableName = tokens[0];
			String colName = tokens[1];
			String isClustered = tokens[2];
			String order = tokens[3];
			
			String key = tableName + "." + colName;
			String value = dbDir + File.separator + "indexes" + File.separator + key;
			
			indexDir.put(key, value);
			// TODO changed for project 5 (untested)
			if(relToIndex.containsKey(tableName)) {
				relToIndex.get(tableName).add(colName);
			}
			else {
				ArrayList<String> colNames = new ArrayList<String>();
				colNames.add(colName);
				relToIndex.put(tableName, colNames);
			}
			Boolean isClusteredBool = isClustered.equals("1");
			relToClustered.put(tableName, isClusteredBool);
			buildIndex(value, tokens);
		}
	}

	/**
	 * This is where all of the logic behind building an index is.
	 * The general steps are:
	 * 	0. generate data entries
	 *  1. create leafnode layer from data entries
	 *  2. create first indexnode layer from the leafnodes
	 *  3. continue creating indexnode layers until we reach the root
	 * When each layer is created, we serialize this to the index file as appropriate.
	 * @param dir
	 * @param tokens
	 */
	@SuppressWarnings("unchecked")
	private static <K extends Comparable<K>,T> void buildIndex(String dir, String[] tokens) {
		entryIndex = 0;
		k = 0;
		currentAddr = 1;
		
		String tableName = tokens[0];
		String attrName = tokens[1];
		boolean isClustered = tokens[2].equals("1");
		int D = Integer.parseInt(tokens[3]);
		IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> root = null;
		
		ArrayList<String> sortCol = new ArrayList<String>();
		sortCol.add(tableName + "." + attrName);
		File indexFile = new File(dir);		//don't delete!
		TupleWriter tw = new TupleWriter(dir);
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
			ArrayList<Integer> address = new ArrayList<Integer>();
			address.add(1);
			root = new IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>();
			root.setLeafChildren(leaves);
			root.setChildrenAddresses(address);
			root.setAddress(2);
			ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> rootI = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
			rootI.add(root);
			
			//serialize this puny tree
			serializeHeader(tw, D);
			serializeLeaves(tw);
			serializeOneLayer(tw, rootI);
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
			//create leaf addresses, 1...leaves.size()
			ArrayList<Integer> leafAddresses = new ArrayList<Integer>();
			for (int i = 1; i <= leaves.size(); i++)
				leafAddresses.add(i);
			
			//serialize the header page, then the leaves
			serializeHeader(tw, D);
			serializeLeaves(tw);
			
			//2. create index node layers until have root (1 IndexNode), using ArrayList<LeafNode<...>> leaves
			anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
			m = leaves.size();
			//create first IndexNode layer and return the number in that layer to set up for the next
			m = createFirstIndexLayer(D, leafAddresses, tw);
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
				
				serializeOneLayer(tw, anIndexLayer);
				//update size of latest layer (for checking if we're at the root (size=1))
				m = anIndexLayer.size();
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
		ArrayList<Integer> keys = new ArrayList<Integer>();
		ArrayList<ArrayList<RecordID>> entries = new ArrayList<ArrayList<RecordID>>();
		leaves = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
		for (DataEntry de : allDataEntries) {
			keys.add(de.getSortKey());
			entries.add(de.getRecIDs());
		}
		LeafNode<Integer, ArrayList<RecordID>> l = new LeafNode<Integer, ArrayList<RecordID>>(keys, entries);
		l.setAddress(1);
		leaves.add(l);
	}

	/**
	 * Helper function to create a new LeafNode after adding the right keys and entries; add to leaves (leaf layer).
	 * @param start
	 * @param end
	 */
	private static void addKeysAndEntries(int start, int end) {
		ArrayList<Integer> keys = new ArrayList<Integer>();
		ArrayList<ArrayList<RecordID>> entries = new ArrayList<ArrayList<RecordID>>();
		for (int i = start; i < end; i++) {
			keys.add(allDataEntries.get(entryIndex).getSortKey());
			entries.add(allDataEntries.get(entryIndex).getRecIDs());
			entryIndex++;
			k--;
		}
		LeafNode<Integer, ArrayList<RecordID>> l = new LeafNode<Integer, ArrayList<RecordID>>(keys, entries);
		l.setAddress(currentAddr);
		currentAddr++;
		leaves.add(l);
	}

	/**
	 * Helper function to create the first layer of IndexNodes from the layer of LeafNodes.
	 * @param D
	 * @return new m (number of IndexNodes in this first layer)
	 */
	private static int createFirstIndexLayer(int D, ArrayList<Integer> lAddrs, TupleWriter tw) {
		entryIndex = 0;
		anIndexLayer = new ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>>();
		
		int numIndexNodes = m / (2 * D + 1);
		if (m % (2 * D + 1) != 0)
			numIndexNodes++;
		//fill all IndexNodes normally except last 2
		while (numIndexNodes > 2) {
			addLeafChildrenAndKeys(0, 2*D+1, lAddrs);
			numIndexNodes--;
		}
		if (m <= (2 * D + 1))				//aka, if numIndexNode = 1; just fill one IndexNode
			addLeafChildrenAndKeys(entryIndex, leaves.size(), lAddrs);
		else if (m >= (3 * D + 2)) {		//fill last 2 IndexNodes as normal
			addLeafChildrenAndKeys(0, 2*D+1, lAddrs);						//second-to-last IndexNode, holding 2D + 1 children
			addLeafChildrenAndKeys(entryIndex, leaves.size(), lAddrs);		//last IndexNode, holding remainder children (at least D + 1 children)
		}
		else {						//construct last 2 IndexNodes so that first get
			int divide = m/2;
			addLeafChildrenAndKeys(0, divide, lAddrs);						//second-to-last IndexNode, holding m/2 children
			addLeafChildrenAndKeys(entryIndex, leaves.size(), lAddrs);		//last IndexNode, holding remainder children
		}
		
		serializeOneLayer(tw, anIndexLayer);
		//update size of latest layer (for checking if we're at the root (size=1))
		return anIndexLayer.size();
	}

	/**
	 * Helper function to create a new IndexNode after adding the right children and keys,
	 * SPECIFICALLY for the first layer of IndexNodes above the LeafNode layer.
	 * @param i
	 * @param j
	 */
	private static void addLeafChildrenAndKeys(int start, int end, ArrayList<Integer> lAddrs) {
		ArrayList<Integer> keys = new ArrayList<Integer>();
		ArrayList<Node<Integer, ArrayList<RecordID>>> leafchildren = new ArrayList<Node<Integer, ArrayList<RecordID>>>();
		ArrayList<Integer> addrs = new ArrayList<Integer>();
		
		//add first child, so that #children = m + 1 and #keys = m
		leafchildren.add(leaves.get(entryIndex));
		addrs.add(leaves.get(entryIndex).getAddress());
		entryIndex++;
		m--;
		for (int i = start + 1; i < end; i++) {
			keys.add(((LeafNode<Integer, ArrayList<RecordID>>)leaves.get(entryIndex)).getFirstKey());
			leafchildren.add(leaves.get(entryIndex));
			addrs.add(lAddrs.get(entryIndex));
			entryIndex++;
			m--;
		}
		IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> in = new IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>();
		in.setKeys(keys);
		in.setLeafChildren(leafchildren);
		in.setChildrenAddresses(addrs);
		in.setAddress(currentAddr);
		currentAddr++;
		anIndexLayer.add(in);
	}
	
	/**
	 * Helper function to create a new IndexNode after adding the right children and keys.
	 * @param i
	 * @param j
	 */
	private static void addChildrenAndKeys(ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>> lastLayer, int start, int end) {
		ArrayList<Integer> keys = new ArrayList<Integer>();
		ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>> children = new ArrayList<Node<Integer, Node<Integer, ArrayList<RecordID>>>>();
		ArrayList<Integer> addrs = new ArrayList<Integer>();
		//add first child, so that #children = m + 1 and #keys = m
		children.add(lastLayer.get(entryIndex));
		addrs.add(lastLayer.get(entryIndex).getAddress());
		entryIndex++;
		m--;
		for (int i = start + 1; i < end; i++) {
			keys.add(((IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>)lastLayer.get(entryIndex)).getFirstKey());
			children.add(lastLayer.get(entryIndex));
			addrs.add(lastLayer.get(entryIndex).getAddress());
			entryIndex++;
			m--;
		}
		IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> in = new IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>();
		in.setKeys(keys);
		in.setIndexChildren(children);
		in.setChildrenAddresses(addrs);
		in.setAddress(currentAddr);
		currentAddr++;
		anIndexLayer.add(in);
	}
	
	/**
	 * Helper method that does the work of serializing the Header page.
	 * @param tw
	 * @param D
	 */
	private static void serializeHeader(TupleWriter tw, int D) {
		int totalLeaves = leaves.size();
		//calculate number of IndexNodes
		int totalIndexes = 0;
		int m = totalLeaves;
		while (m > 1) {
			int temp = m;
			temp = m / (2 * D + 1);
			if (m % (2 * D + 1) != 0)
				temp++;
			totalIndexes += temp;
			m = temp;
		}
		
		int rootAddr = totalLeaves + totalIndexes;
		tw.writeOneInt(rootAddr);		//address of root
		tw.writeOneInt(totalLeaves);	//number of leaves
		tw.writeOneInt(D);				//order of tree
		tw.writeNewPageIndex();
	}
	
	/**
	 * Helper function that does the work of serializing all of the leaves.
	 * @param tw
	 */
	private static void serializeLeaves(TupleWriter tw) {
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
	}
	
	/**
	 * Helper function that does the work of serializing ONE LAYER OF INDEX NODES.
	 * @param tw
	 * @param layer
	 */
	private static void serializeOneLayer(TupleWriter tw, ArrayList<IndexNode<Integer, Node<Integer, ArrayList<RecordID>>>> layer) {
		for (int i = 0; i < layer.size(); i++) {
			IndexNode<Integer, Node<Integer, ArrayList<RecordID>>> index = layer.get(i);
			tw.writeOneInt(1);							//flag for IndexNode
			ArrayList<Integer> keys = index.getKeys();
			tw.writeOneInt(keys.size());				//number of keys in node
			tw.writeManyInts(keys);						//actual keys in node, in order
			tw.writeManyInts(index.getAddresses());		//addresses of all children of node, in order
			tw.writeNewPageIndex();
		}
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
	 * @param relation Name of relation.
	 * @return list of names of columns that have indexes on them for this relation.
	 */
	public ArrayList<String> getIndexCols(String relation) {
		if (relToIndex.containsKey(relation))
			return relToIndex.get(relation);
		else
			return null;
	}
	
	/**
	 * Getter method for the whether a given index is clustered.
	 * @param relation Name of the relation on which we have an index.
	 * @return True if this index exists and is clustered.
	 */
	public boolean getClustered(String relation) {
		if (relToClustered.containsKey(relation))
			return relToClustered.get(relation);
		else
			return false;
	}
}
