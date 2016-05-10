package logical;

import java.util.ArrayList;
import java.util.HashMap;

import code.MyUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class UnionFind {

	private HashMap<String,UnionFindElement> elementOfAttr;
	private Expression unusable;
	
	public UnionFind() {
		elementOfAttr = new HashMap<String,UnionFindElement>();
		unusable = null;
	}
	
	public UnionFindElement find(String attr) {
		if(elementOfAttr.containsKey(attr))
			return elementOfAttr.get(attr);
		// TODO initialize the lowBound/highBound/equality?
		UnionFindElement el = new UnionFindElement();
		el.addAttribute(attr);
		return el;
	}
	
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
			int el1Low = el1.getLowBound();
			int el2Low = el2.getLowBound();
			if (el1Low > el2Low) {
				merge.setIfLowBound(el1Low);
			}
			else {
				merge.setIfLowBound(el2Low);
			}
			int el1High = el1.getHighBound();
			int el2High = el2.getHighBound();
			if (el1High < el2High) {
				merge.setIfHighBound(el1High);
			}
			else {
				merge.setIfHighBound(el2High);
			}
		}
	}
	
	public Expression getUnusable() {
		return unusable;
	}
	
	public void addToUnusable(Expression ex) {
		unusable = MyUtils.safeConcatExpression(unusable, ex);
	}
}
