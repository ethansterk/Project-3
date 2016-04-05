package code;
/**
 * The WriteOutput maintains a single PrintStream that will output the query files.
 * 
 * It acts as a global entity that various components of the system may want
 * to access, so it uses the singleton pattern.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 * 
 * see https://en.wikipedia.org/wiki/Singleton_pattern
 *
 */
public class OutputWriter {

	private static final OutputWriter instance = new OutputWriter();
	private static String outputDir = "";
	private static String tempDir = "";
	private static int queryNumber = 0;
	
	/**
	 * Since there is only one instance of the WriteOutput, there is no need
	 * for a public constructor.
	 */
	private OutputWriter() {

	}
	
	/**
	 * Creates the WriteOutput. This is called only once, from the
	 * main() function in Parser.
	 * 
	 * @param outputDir
	 */
	public static void createStream(String outputDirectory) {
	    outputDir = outputDirectory;
	}
	
	/**
	 * Creates the stream for the temporary directory used in external sort merge.
	 * This is called only once, from the main() function in Parser.
	 * 
	 * @param tempDir
	 */
	public static void createTempStream(String tempDirectory) {
		tempDir = tempDirectory;
	}
	
	/**
	 * Access method to return the one and only instance of DatabaseCatalog.
	 * 
	 * @return OutputWriter
	 */
	public static OutputWriter getInstance() {
		return instance;
	}
	
	/**
	 * Access method to return the output directory.
	 * 
	 * @return outputDir
	 */
	public String getOutputDir() {
		return outputDir;
	}
	
	/**
	 * Access method to return the temp. directory.
	 * 
	 * @return tempDir
	 */
	public String getTempDir() {
		return tempDir;
	}
	
	/**
	 * Access method to return the current query number.
	 * 
	 * @return queryNumber
	 */
	public int getQueryNumber() {
		return queryNumber;
	}
	
	/**
	 * Setter method that increments the queryNumber.
	 */
	public void increment() {
		queryNumber++;
	}
}
