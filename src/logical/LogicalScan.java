package logical;

public class LogicalScan extends LogicalOperator{
	private String tablename;
	
	public LogicalScan(String tablename) {
		this.tablename = tablename;
	}
	
	public String getTablename() {
		return tablename;
	}

	public void accept(PhysicalPlanBuilder visitor) {
		visitor.visit(this);
	}
}
