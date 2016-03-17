import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class CheckOutputs {
	
	//have to change these three values depending on what we're running and where we've put the results
	private int numFiles = 8;				
	private String expectedDir = "expected_output";
	private String testDir = "output";
	
	private File expected;
	private File testfile;

	@Test
	public void test() {		
		for (int i = 1; i < numFiles; i++) {
			expected = new File(expectedDir + File.separator + "query" + i);
			testfile = new File(testDir + File.separator + "query" + i);
			
			boolean compare;
			try {
				compare = compareTwoFiles(expected, testfile);
				assertEquals(true, compare);
			} catch (IOException e) {
				e.printStackTrace();
				assertEquals(true, false);
			}
		}
	}

	//http://stackoverflow.com/questions/21764299/comparing-two-files-in-java
	public boolean compareTwoFiles(File one, File two) throws IOException {

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
}
