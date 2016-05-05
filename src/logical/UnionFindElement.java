package logical;

import java.util.ArrayList;

public class UnionFindElement {

	private ArrayList<String> attributes;
	private int lowBound;
	private int highBound;
	private Integer equalityConstr;
	
	public UnionFindElement() {
		attributes = new ArrayList<String>();
		lowBound = Integer.MAX_VALUE;
		highBound = Integer.MIN_VALUE;
		equalityConstr = null;
	}

	public ArrayList<String> getAttributes() {
		return attributes;
	}

	public void clearAttributes() {
		this.attributes = new ArrayList<String>();
	}
	
	public void addAttribute(String attr) {
		this.attributes.add(attr);
	}
	
	public void addAllAttributes(ArrayList<String> attrs) {
		this.attributes.addAll(attrs);
	}

	public int getLowBound() {
		return lowBound;
	}

	public void setLowBound(int lowBound) {
		this.lowBound = lowBound;
	}

	public int getHighBound() {
		return highBound;
	}

	public void setHighBound(int highBound) {
		this.highBound = highBound;
	}

	public Integer getEqualityConstr() {
		return equalityConstr;
	}

	public void setEqualityConstr(Integer equalityConstr) {
		this.equalityConstr = equalityConstr;
	}
}
