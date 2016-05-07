package logical;

import java.util.ArrayList;
import java.util.HashMap;

import code.MyUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class UnionFindElement {

	private ArrayList<String> attributes;
	private int lowBound;
	private int highBound;
	private Integer equalityConstr;
	
	private HashMap<String,Expression> exprOfRel;
	
	public UnionFindElement() {
		attributes = new ArrayList<String>();
		lowBound = Integer.MAX_VALUE;
		highBound = Integer.MIN_VALUE;
		equalityConstr = null;
		
		exprOfRel = new HashMap<String,Expression>();
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

	public void setIfLowBound(int lowBound) {
		if (lowBound > this.lowBound)
			this.lowBound = lowBound;
	}

	public int getHighBound() {
		return highBound;
	}

	public void setIfHighBound(int highBound) {
		if (highBound < this.highBound)
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
	
	// TODO This below might not work well because it relies on getting epxressions for
	// each relation, and this is difficult in the visitor.
	public void addExpressionToRelation(String relName, Expression e) {
		if (exprOfRel.get(relName) == null)
			exprOfRel.put(relName, e);
		else {
			Expression tempE = exprOfRel.get(relName);
			MyUtils.safeConcatExpression(tempE, e);
			exprOfRel.put(relName, tempE);
		}
	}
	
	public Expression getExpressionOfRelation(String relName) {
		return exprOfRel.get(relName);
	}
}
