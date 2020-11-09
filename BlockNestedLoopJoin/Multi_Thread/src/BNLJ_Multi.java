import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.concurrent.TimeUnit;

public class BNLJ_Multi {
    public static void main(String[] args) throws Exception {
        /* Format of 'args' : outerTableFile c1 innerTableFile c2 numberWorkers numTupleByBlock bufferSize joinTableFile */
        /* Ex of 'args'     : data/purchases_heavy.csv 0 data/clients_heavy.csv 0 4 200 8132 data/results.csv */
		
        /*  Tables Files */
        String outerTableFile               = "./data/purchases_heavy.csv";
        String innerTableFile               = "./data/clients_heavy.csv";
        String joinTableFile                = "./data/results5.csv";

        /** Config Variables   **/
        BufferedReader bufferOuterRelation  = null;
        BufferedWriter bufferJoinRelation   = null;
        Block outerBlock                    = null;
        int numberWorkers                   = 4;
        int numTupleByBlock                 = 200; 
        int bufferSize                      = 8*1024;
        int c1                              = 0;
        int c2                              = 0;
        long startTime                      = System.nanoTime();
        long stopTime                       = 0;
        Workers t[]                         = new Workers[numberWorkers];

        try {
			if (args.length == 8) {
                outerTableFile  = args[0];
                c1              = Integer.parseInt(args[1]);
                innerTableFile  = args[2];
                c2              = Integer.parseInt(args[3]);
                numberWorkers   = Integer.parseInt(args[4]);
                numTupleByBlock = Integer.parseInt(args[5]);
                bufferSize      = Integer.parseInt(args[6]);
                joinTableFile   = args[7];
			} else if(args.length != 0){
				throw new Exception("There are not enough arguments!");
			}
		} catch (Exception e) {
			e.printStackTrace();
        }
        
        // Create the Buffers
        bufferOuterRelation     = new BufferedReader(new FileReader(outerTableFile),bufferSize);
        bufferJoinRelation      = new BufferedWriter(new FileWriter(joinTableFile));

        // Create the Block for the outer relation
        outerBlock              = new Block(bufferOuterRelation, numTupleByBlock );
        System.out.println("\tPrograma starts");
     
        // Create the Threads
        for (int i = 0;i < numberWorkers;i++){
            t[i] = new Workers ("T"+Integer.toString(i),outerBlock,bufferJoinRelation,innerTableFile,bufferSize,c1,c2);
        }

        // Execute the Threads
        for (int i = 0;i < numberWorkers;i++){
            t[i].start();
            System.out.println("Thread "+ t[i]+" starts");
        }

        // Wait for the end of Threads
        for (int i = 0;i < numberWorkers;i++){
            t[i].join();
            System.out.println("Thread "+ t[i]+" is finish");
        }
        
        // Close the buffers
        bufferOuterRelation.close();
        bufferJoinRelation.close();
      
        stopTime    = System.nanoTime();
        System.out.println("Time: " + TimeUnit.MILLISECONDS.convert(stopTime-startTime,TimeUnit.NANOSECONDS)+" ms");

        long memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		int memoryUsedInMB = (int) ((double) memoryUsed / 1048576);
		System.out.println("Total memory used: " + memoryUsedInMB + " MB");	
    }
}
