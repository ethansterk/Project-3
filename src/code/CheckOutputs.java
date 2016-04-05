package code;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Test suite for comparing expected query results to what we're outputting.
 * The first three class variables (numFiles, expectedDir, and testDir) have to be
 * put in manually because we are unable to access args from this class.
 * 
 * @author Ethan (ejs334) and Laura (ln233)
 *
 */
public class CheckOutputs {
	
	//have to change these three values depending on what we're running and where we've put the results
	private int numFiles = 15;	
	// TODO switch directories after pulling code
	//private String expectedDir = "C:" + File.separator + "Users" + File.separator + "Ryu" + File.separator + "Desktop" + File.separator + "P3" + File.separator + "expected";
	private String expectedDir = "C:"+File.separator+"Users"+File.separator+"Ethan"+File.separator+"Desktop"+File.separator+"samples"+File.separator+"expected";
	//private String testDir = "C:" + File.separator + "Users" + File.separator + "Ryu" + File.separator + "Desktop" + File.separator + "P3" + File.separator+ "output";
	private String testDir = "C:"+File.separator+"Users"+File.separator+"Ethan"+File.separator+"Desktop"+File.separator+"samples"+File.separator+"output";
	
	private File expected;
	private File testfile;
	
	private boolean project_three_io = true;

	@Test
	public void test() {		
		for (int i = 1; i <= numFiles; i++) {
			expected = new File(expectedDir + File.separator + "query" + i);
			testfile = new File(testDir + File.separator + "query" + i);
			System.out.println("query " + i);
			
			boolean compare;
			try {
				if (project_three_io) 
					compare = CompareTwoFilesbyByte(expected, testfile);
				else 
					compare = compareTwoFilesHR(expected, testfile);			
				assertEquals(true, compare);
			} catch (Exception e) {
				e.printStackTrace();
				assertEquals(true, false);
			}
		}
	}

	//http://stackoverflow.com/questions/21764299/comparing-two-files-in-java
	public boolean compareTwoFilesHR(File one, File two) throws IOException {
	    BufferedReader br1 = new BufferedReader(new FileReader(one));
	    BufferedReader br2 = new BufferedReader(new FileReader(two));
	
	    String thisLine = null;
	    String thatLine = null;
	
	    List<String> list1 = new ArrayList<String>();
	    List<String> list2 = new ArrayList<String>();
	
	    while ((thisLine = br1.readLine()) != null) {
	        list1.add(thisLine);
	    }
	    while ((thatLine = br2.readLine()) != null) {
	        list2.add(thatLine);
	    }
	    
	    br1.close();
	    br2.close();
	
	    return list1.equals(list2);
	}
	
	//http://javaonlineguide.net/2014/10/compare-two-files-in-java-example-code.html
	@SuppressWarnings("resource")
	public boolean CompareTwoFilesbyByte(File one, File two) throws IOException
    {
        FileInputStream fis1 = new FileInputStream(one);
        FileInputStream fis2 = new FileInputStream(two);
        if (one.length() == one.length())
            {
                int n=0;
                byte[] b1;
                byte[] b2;
                while ((n = fis1.available()) > 0) {
                    if (n>4096) n=4096;
                    b1 = new byte[n];
                    b2 = new byte[n];
                    fis1.read(b1);
                    fis2.read(b2);
                    if (Arrays.equals(b1,b2)==false)
                        {
                            return false;
                        }
                }
            }
        else return false;  // length is not matched. 
        return true;
    }
}
