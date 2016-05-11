package logical;

import java.util.ArrayList;

public class UnionFindElement {

	private ArrayList<String> attributes;
	private Integer lowBound;
	private Integer highBound;
	private Integer equalityConstr;
	
	public UnionFindElement() {
		attributes = new ArrayList<String>();
		lowBound = null;
		highBound = null;
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

	public Integer getLowBound() {
		return lowBound;
	}

	public void setIfLowBound(int lowBound) {
		if (this.lowBound == null)
			this.lowBound = lowBound;
		else if (lowBound > this.lowBound)
			this.lowBound = lowBound;
	}

	public Integer getHighBound() {
		return highBound;
	}

	public void setIfHighBound(int highBound) {
		if (this.highBound == null)
			this.highBound = highBound;
		else if (highBound < this.highBound)
			this.highBound = highBound;
	}

	public Integer getEqualityConstr() {
		return equalityConstr;
	}

	public void setEqualityConstr(Integer equalityConstr) {
		this.equalityConstr = equalityConstr;
		this.lowBound = equalityConstr;
		this.highBound = equalityConstr;
	}
}
