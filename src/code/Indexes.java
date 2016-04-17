package code;

public class Indexes {

	private static final Indexes instance = new Indexes();
	
	public static Indexes getInstance() {
		return instance;
	}
	
	private Indexes() {
		
	}
	
	public static void createIndexes(String dbDir, boolean build) {
		//extract from dbDir
		if (build) {
			buildIndexes();
		}
	}

	private static void buildIndexes() {
		// TODO Auto-generated method stub
		
	}
}
