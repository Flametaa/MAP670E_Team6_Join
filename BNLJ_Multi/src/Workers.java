import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Workers extends Thread {
    /* Static Variables */
    private static BufferedWriter bufferJoinRelation    = null;
    private static Block outerBlock                     = null;
    private static Boolean isEmpty                      = false;
    private static int c1,c2;

    /* Variables */
    private BufferedReader bufferInnerRelation          = null;
    private ArrayList<Tuple> outerTuples                = new ArrayList<Tuple>();
    private Tuple innerTuple                            = null;
    private String innerTableFile                       = null;
    private int innerBufferSize;
    

    public Workers(String name, Block outerBlock, BufferedWriter bufferJoinRelation, String innerTableFile, int innerBufferSize, int c1, int c2) {
        super(name);
        Workers.outerBlock          = outerBlock;
        Workers.bufferJoinRelation  = bufferJoinRelation;
        Workers.c1                  = c1;
        Workers.c2                  = c2;
        this.innerTableFile         = innerTableFile;
        this.innerBufferSize        = innerBufferSize;
    }
    
    public void run() {
        
        while(!isEmpty){
            // Load the outer Block and create the buffer of inner Relation
            try {
                isEmpty             = outerBlock.loadBlock(outerTuples);
                bufferInnerRelation = new BufferedReader(new FileReader(innerTableFile),innerBufferSize);
                innerTuple          = new Tuple(bufferInnerRelation.readLine());
                if(outerTuples.size() == 0) break;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
            // Nested Loop Join Algorithm
            while(innerTuple.getNumFields()!=0){
                for (Tuple outerTuple : outerTuples){
                    JoinOperation.exec(outerTuple,innerTuple,bufferJoinRelation,2,c1,c2);
                }
                
                // Take next tuple of inner Relation
                try {
                    innerTuple = new Tuple(bufferInnerRelation.readLine());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // Close buffer
            try {
                bufferInnerRelation.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // Close buffer
        try {
            bufferInnerRelation.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
              
    }
}
