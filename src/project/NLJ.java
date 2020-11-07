package project;
import java.io.BufferedReader;
import java.io.BufferedWriter;
/**
 * 
 * This class correpond to the implementation of the algorithm Nested Loop Join using 
 * the clasic join. Moreover this class was implemented with the help of theory in this site web
 * www.geeksforgeeks.org/join-algorithms-in-database/
 */
public class NLJ {
	
	/** Simple NLJ  **/
	public void nestedLoopJoin(String R_csv, String S_csv,int col1, int col2, String output_join) {
		
		String R_Name = Relations_File.getName(R_csv);  // Get the name of the R relation from the csv file name
		String S_Name = Relations_File.getName(S_csv); // Get the name of the S relation from the csv file name
		
		BufferedReader br1 = Relations_File.createBufferedReaderAndSkipFirstLine(R_csv);
		BufferedReader br2 = Relations_File.createBufferedReaderAndSkipFirstLine(S_csv);
		BufferedWriter bw = Relations_File.createBufferedWriter(output_join);
		boolean header = true; // boolean used to print the header

		Tuple r = null;
		Tuple s = null;
		while ((r = Relations_File.getNextTuple(br1, R_Name)) != null) {
			while ( (s = Relations_File.getNextTuple(br2, S_Name)) != null) {
				if (r.getAttribute(col1).getValue().compareTo(s.getAttribute(col2).getValue())==0) {
					Tuple outputTuple = Tuple.joinTuples(r, s, col1, col2);
					
					// we should write the attributes header beforehand 
					if (header) {
						Relations_File.writeHeader(bw, outputTuple);
						header = false;
					}
					Relations_File.writeTuple(bw, outputTuple);
				}
			}
			Relations_File.closeBufferedReader(br2);
			br2 = Relations_File.createBufferedReaderAndSkipFirstLine(S_csv);
		}
		Relations_File.closeBufferedReader(br1);
		Relations_File.closeBufferedWriter(bw);
	}
	
}
	
