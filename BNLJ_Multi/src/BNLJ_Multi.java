import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class BNLJ_Multi {
    public static void main(String[] args) throws Exception {
        /*  Tables  */
        String outerTableFile               = "./data/posts.csv";
        String innerTableFile               = "./data/authors.csv";
        String joinTableFile                = "./data/results2.csv";

        /** Variables   **/
        BufferedReader bufferOuterRelation  = null;
        BufferedWriter bufferJoinRelation   = null;
        Block outerBlock                    = null;
        int numberWorkers                   = 4;
        int outerBlockSize                  = 500; 
        int innerBlockSize                  = 1;
        int outerBufferSize                 = 1024*outerBlockSize;
        int innerBufferSize                 = 1*innerBlockSize;
        long startTime                      = System.nanoTime();
        long stopTime                       = 0;
        Workers t[]                         = new Workers[numberWorkers];

        try{
            bufferOuterRelation     = new BufferedReader(new FileReader(outerTableFile));
            bufferJoinRelation      = new BufferedWriter(new FileWriter(joinTableFile));
            outerBlock              = new Block(bufferOuterRelation, outerBlockSize );
            Timestamp timestamp     = new Timestamp(System.currentTimeMillis());
            System.out.println(timestamp + " Inicio del programa");
        }
        catch(IOException e){
            System.out.println("Could not read the file...");
            e.printStackTrace();
        }

        for (int i = 0;i < numberWorkers;i++){
            t[i] = new Workers ("T"+Integer.toString(i),outerBlock,bufferJoinRelation,innerTableFile,innerBufferSize);
        }
        for (int i = 0;i < numberWorkers;i++){
            t[i].start();
            System.out.println("Thread "+ t[i]+" starts");
        }
        for (int i = 0;i < numberWorkers;i++){
            t[i].join();
            System.out.println("Thread "+ t[i]+" is finish");
        }
        
      
        bufferOuterRelation.close();
        bufferJoinRelation.close();
      
        stopTime    = System.nanoTime();
        System.out.println("Time: " + TimeUnit.SECONDS.convert(stopTime-startTime,TimeUnit.NANOSECONDS));
    }
}
