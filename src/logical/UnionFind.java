package logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import code.MyUtils;
import net.sf.jsqlparser.expression.Expression;

public class UnionFind {

	private HashMap<String,UnionFindElement> elementOfAttr;
	private Expression unusable;
	
	/**
	 * Initialize the UnionFind object.
	 */
	public UnionFind() {
		elementOfAttr = new HashMap<String,UnionFindElement>();
		unusable = null;
	}
	
	/**
	 * find method returns the element of the given attribute if one
	 * exists. If one does not already exist, then one is created.
	 * @param attr
	 * @return
	 */
	public UnionFindElement find(String attr) {
		if(elementOfAttr.containsKey(attr))
			return elementOfAttr.get(attr);
		UnionFindElement el = new UnionFindElement();
		el.addAttribute(attr);
		elementOfAttr.put(attr,el);
		return el;
	}
	
	/**
	 * union merges two union find elements into one element.
	 * @param el1
	 * @param el2
	 */
	public void union(UnionFindElement el1, UnionFindElement el2) {
		UnionFindElement merge = new UnionFindElement();
		
		ArrayList<String> el1Attrs = el1.getAttributes();
		ArrayList<String> el2Attrs = el2.getAttributes();
		for (String attr : el1Attrs) {
			elementOfAttr.put(attr, merge);
		}
		for (String attr : el2Attrs) {
			elementOfAttr.put(attr, merge);
		}
		
		merge.addAllAttributes(el1Attrs);
		merge.addAllAttributes(el2Attrs);
		
		Integer el1EQ = el1.getEqualityConstr();
		Integer el2EQ = el2.getEqualityConstr();
		if (el1EQ != null) {
			merge.setEqualityConstr(el1EQ);
			merge.setIfLowBound(el1EQ);
			merge.setIfHighBound(el1EQ);
		}
		else if (el2EQ != null) {
			merge.setEqualityConstr(el2EQ);
			merge.setIfLowBound(el2EQ);
			merge.setIfHighBound(el2EQ);
		}
		else {
			Integer el1Low = el1.getLowBound();
			Integer el2Low = el2.getLowBound();
			if (el1Low != null)
				merge.setIfLowBound(el1Low);
			if (el2Low != null)
				merge.setIfLowBound(el2Low);
			Integer el1High = el1.getHighBound();
			Integer el2High = el2.getHighBound();
			if (el1High != null)
				merge.setIfHighBound(el1High);
			if (el2High != null)
				merge.setIfHighBound(el2High);
		}
	}
	
	/**
	 * returns the unusable conditions
	 * @return
	 */
	public Expression getUnusable() {
		return unusable;
	}
	
	/**
	 * functions to append an Expression to the unusable condition
	 * @param ex
	 */
	public void addToUnusable(Expression ex) {
		unusable = MyUtils.safeConcatExpression(unusable, ex);
	}

	/**
	 * functions that returns all the union find elements of this given
	 * union find
	 * @return
	 */
	public ArrayList<UnionFindElement> getElements() {
		ArrayList<UnionFindElement> vals = new ArrayList<UnionFindElement>();
		HashSet<UnionFindElement> valsSet = new HashSet<UnionFindElement>();
		for (String key: elementOfAttr.keySet()) {
		    valsSet.add(elementOfAttr.get(key));
		}
		vals.addAll(valsSet);
		return vals;
	}
}
