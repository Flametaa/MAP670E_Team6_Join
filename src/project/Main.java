package project;
/**
 * Final
 Main function for executing Single Nested Loop Join and Block Nested Loop Join algorithms
 @parameters
 *args: argument in the form of a string of characters to choose the relations R,S,columns for 
        make the join, the type of algorithm to test and the name of the csv file to keep our result.
 */

public class Main {
	
	public static void main(String[] args) { 
		//Format of 'args': -R database_created/D.csv -c1 0 -S database_created/C.csv -c2 2 -t BNLJ -n_tup_bloc 200 -o results.csv//
		
		String R_csv = "";                       //File of relation R in format .csv
		String S_csv = "";                      //File of relation S in format .csv
		int col1 = 0;                          //column or attribute of   R
		int col2 = 0;                         //column or attribute of   S
		String joinAlgorithm = "";           //Type of Algorithm (NLJ or BNLJ)
		int n_tup_bloc = 0;                 // number of tuples  per bloc
		String output_T = "";              //file output-result of join relation R and S
		
		try {
			if (args.length >= 11) {
				for (int i=0; i<args.length; i++) {
				    if (args[i].equals("-R")){
						if (i == args.length-1) {
							throw new Exception("misuse of -R. This should not be the last argument!");
						}
						R_csv = args[i+1];
				    }
				    if (args[i].equals("-S")) {
						if (i == args.length-1) {
							throw new Exception("misuse of -S. This should not be the last argument!");
						}
						S_csv = args[i+1];
				    }
				    if (args[i].equals("-c1")) {
						if (i == args.length-1) {
							throw new Exception("misuse of -c1. This should not be the last argument!");
						}
						col1 = Integer.parseInt(args[i+1]);
				    }
				    if (args[i].equals("-c2")) {
						if (i == args.length-1) {
							throw new Exception("misuse of -c2. This should not be the last argument!");
						}
						col2 = Integer.parseInt(args[i+1]);
				    }
				    if (args[i].equals("-t")) {
						if (i == args.length-1) {
							throw new Exception("misuse of -t. This should not be the last argument!");
						}
						joinAlgorithm = args[i+1];
				    }
				    if (args[i].equals("-n_tup_bloc")) {
						if (i == args.length-1) {
							throw new Exception("misuse of -m. This should not be the last argument!");
						}
						n_tup_bloc = Integer.parseInt(args[i+1]);
				    }
				   
				    if (args[i].equals("-o")) {
						if (i == args.length-1) {
							throw new Exception("misuse of -o. This should not be the last argument!");
						}
						output_T = args[i+1];
				    }
				}
			} else {
				throw new Exception("There are not enough arguments!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
					
		// get the name of the relation R from the R_csv file name
		String relation1Name = Relations_File.getName(R_csv);
		
		// get the name of the relation S from the S_csv file name
		String relation2Name = Relations_File.getName(S_csv);
		
		
		int T1 = Relations_File.getNumberRows(R_csv);   // number of records in relation R
		int T2 = Relations_File.getNumberRows(S_csv);  // number of records in relation S

		System.out.println("Relation " + relation1Name + " number of records: " + T1);
		System.out.println("Relation " + relation2Name + " number of records: " + T2);
		System.out.println();
		
		/*** EXECUTING JOIN ***/
			
		 if ( joinAlgorithm.equals("NLJ") ) {
			
			/*** Simple Nested Loop Join ***/
			
			System.out.println("\"Executing a simple nested loop join...\"");
			NLJ nlj = new NLJ();
			
			// Nested Loop Join R, S on col1=col2
			
			long nljStartTime = System.currentTimeMillis();
			nlj.nestedLoopJoin(R_csv, S_csv,col1, col2, output_T);
			long nljTime = System.currentTimeMillis() - nljStartTime;
			double nljTime_Sec = (double) nljTime / 1000;
			double nlj_cost = T1 * T2;
			
			System.out.println();
			System.out.println("Number of join tuples: " + (Relations_File.countLinesFile(output_T) - 1));
			System.out.println("Total time of NLJ: " + nljTime_Sec + " sec");
			System.out.println("Complexity of NLJ: " +"T(" + relation1Name + ") * " + "T("  + relation2Name + ")");
			System.out.printf("Cost of NLJ: %.0f I/Os", nlj_cost);
			System.out.println();
		}
		 

		 
		 if ( joinAlgorithm.equals("BNLJ") ) {
				
				/***Block Nested Loop Join ***/
				
			System.out.println("Running Block Nested Loop Join...");
			BNLJ bnlj = new BNLJ();
				
				// Block Nested Loop Join R, S on col1=col2
				
			long bnljStartTime = System.currentTimeMillis();
			bnlj.blockNestedLoopJoin(R_csv,S_csv,col1, col2,n_tup_bloc, output_T);
			long bnljTime = System.currentTimeMillis() - bnljStartTime;
			double bnljTime_Sec = (double) bnljTime / 1000;
			//double bnlj_cost = ;
				
			System.out.println();
			System.out.println("Number of join tuples: " + (Relations_File.countLinesFile(output_T) - 1));
			System.out.println("Total time of BNLJ : " + bnljTime_Sec + " sec");
			System.out.println("Complexity of BNLJ: " + "T(" + relation1Name + ") * " + "T("  + relation2Name + ")");
			//System.out.printf(" Cost of disk BNLJ: %.0f I/Os", bnlj_cost);
			System.out.println();
			}
		 
		long memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		int memoryUsedInMB = (int) ((double) memoryUsed / 1048576);
		System.out.println("Total memory used: " + memoryUsedInMB + " MB");	
		}
	}
	
