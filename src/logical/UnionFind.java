package logical;

import java.util.HashMap;

public class UnionFind {

	private HashMap<String,UnionFindElement> elementOfAttr;
	
	public UnionFind() {
		
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
		
		merge.addAllAttributes(el1.getAttributes());
		merge.addAllAttributes(el2.getAttributes());
		
		Integer el1EQ = el1.getEqualityConstr();
		Integer el2EQ = el2.getEqualityConstr();
		if (el1EQ != null) {
			merge.setEqualityConstr(el1EQ);
			merge.setLowBound(el1EQ);
			merge.setHighBound(el1EQ);
		}
		else if (el2EQ != null) {
			merge.setEqualityConstr(el2EQ);
			merge.setLowBound(el2EQ);
			merge.setHighBound(el2EQ);
		}
		else {
			int el1Low = el1.getLowBound();
			int el2Low = el2.getLowBound();
			if (el1Low > el2Low) {
				merge.setLowBound(el1Low);
			}
			else {
				merge.setLowBound(el2Low);
			}
			int el1High = el1.getHighBound();
			int el2High = el2.getHighBound();
			if (el1High < el2High) {
				merge.setHighBound(el1High);
			}
			else {
				merge.setHighBound(el2High);
			}
		}
	}
}
