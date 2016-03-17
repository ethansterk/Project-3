import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class Logger {
	
	private static Logger instance = new Logger();
	private static File log;
	private static boolean firstWrite;
	
	private Logger() {
		
	}
	
	public static void createLogger(String loggerDir) {
		log = new File(loggerDir + File.separator + "log.txt");
		firstWrite = true;
	}
	
	public static Logger getInstance() {
		return instance;
	}
	
	public static void log(String message) {
		//System.out.println("Logging message: " + message);
		
		try{
		    if(!log.exists()){
		        System.out.println("We had to make a new file.");
		        log.createNewFile();
		    }

		    FileWriter fileWriter = new FileWriter(log, !firstWrite);

		    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		    bufferedWriter.write(message + '\n');
		    bufferedWriter.close();

		    //System.out.println("Done");
		} catch(IOException e) {
		    System.out.println("COULD NOT LOG!!");
		}
		
		firstWrite = false;
	}
}
