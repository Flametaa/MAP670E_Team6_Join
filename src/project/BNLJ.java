package project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;
/**final
 * 
 **/
public class BNLJ {
	/** BNLJ. 
	 * Each block contains nb_tup_bloc tuples
	 * We want R to be the smaller one to create less blocks thus R will be the outer relation
	 * **/
	public void blockNestedLoopJoin(String R, String S,int col1, int col2, int nb_tup_bloc, String output_join) {
		
		Relations_File.createNewFile(output_join);
		int Tr = Relations_File.getNumberRows(R); // number of rows or tuples of relation1
		int Ts = Relations_File.getNumberRows(S); // number of rows or tuples of relation2
		if (Ts < Tr) {
			String tempString = R;
			R = S;
			S= tempString;
			int tempInteger = col1;
			col1 = col2;
			col2 = tempInteger;
			tempInteger = Tr;
			Tr = Ts;
			Ts = tempInteger;
		}
		
		String relation1Name = Relations_File.getName(R);  // get the name of the 1st relation from the csv file name
		String relation2Name = Relations_File.getName(S); // get the name of the 2nd relation from the csv file name
		
		if (Ts >= Tr) {
			System.out.println("Outer relation is: " + relation1Name);
			System.out.println("Inner relation is: " + relation2Name);
		} else {
			System.out.println("Outer relation is: " + relation2Name);
			System.out.println("Inner relation is: " + relation1Name);
		}
		
		BufferedReader br1 = Relations_File.createBufferedReaderAndSkipFirstLine(R);
		BufferedReader br2 = Relations_File.createBufferedReaderAndSkipFirstLine(S);
		BufferedWriter bw = Relations_File.createBufferedWriter(output_join);
		boolean header = true; // used to print the header

		List<Tuple> R_outer = new ArrayList<Tuple>(); // block of relation1 (outer relation with less records than relation2)
		
		Tuple r = null;
		Tuple s = null;
		int pR=0; // pointer for relation1
		while ((r = Relations_File.getNextTuple(br1, relation1Name)) != null) {
			
			R_outer.add(r);
			pR++;

			if ( (pR % (nb_tup_bloc-1) == 0) || (pR == Relations_File.getNumberRows(R)) ) {

				while ( (s = Relations_File.getNextTuple(br2, relation2Name)) != null) {
					for (int j=0; j<R_outer.size(); j++) {
						
						if (R_outer.get(j).getAttribute(col1).getValue() == s.getAttribute(col2).getValue()) {
							Tuple outputTuple = Tuple.joinTuples(R_outer.get(j), s, col1, col2);
							if (header) {
								Relations_File.writeHeader(bw, outputTuple);
								header = false;
							}
							Relations_File.writeTuple(bw, outputTuple);
						}
					}
				}
				R_outer.clear();
				Relations_File.closeBufferedReader(br2);
				br2 = Relations_File.createBufferedReaderAndSkipFirstLine(S);
			}
		}
		Relations_File.closeBufferedReader(br1);
		Relations_File.closeBufferedReader(br2);
		Relations_File.closeBufferedWriter(bw);
	}
}
