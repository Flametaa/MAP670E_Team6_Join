import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Workers extends Thread {
    private static BufferedWriter bufferJoinRelation    = null;
    private static BufferedReader bufferOuterRelation   = null;
    private static Block outerBlock                     = null;
    private BufferedReader bufferInnerRelation          = null;
    //private String outerTuple                           = null;
    private ArrayList<String> outerTuples               = new ArrayList<String>();
    private String innerTuple                           = null;
    private String innerTableFile                       = null;
    private int bufferSize;
    private static Boolean isEmpty                      = false;

    public Workers(String name, Block outerBlock, BufferedWriter bufferJoinRelation, String innerTableFile, int innerBufferSize) {
        super(name);
        Workers.outerBlock           = outerBlock;
        Workers.bufferJoinRelation   = bufferJoinRelation;
        this.innerTableFile         = innerTableFile;
        this.bufferSize             = innerBufferSize;
    }
    
    public void run() {
        
        while(!isEmpty){
            String[] innerTupleFields = null;
            try {
                isEmpty = outerBlock.loadBlock(outerTuples);
                bufferInnerRelation = new BufferedReader(new FileReader(innerTableFile));
                innerTuple   = bufferInnerRelation.readLine();
                innerTupleFields = innerTuple.split(",");
                if(outerTuples.size() == 0) break;

            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            while(innerTuple != null){
                
                for (String outerTuple : outerTuples){
                    String outerTupleFields = outerTuple;

                    if(outerTupleFields.equals(innerTupleFields[0]) ){
                        String joinResult = String.join(",",outerTupleFields,innerTupleFields[2],"\n");
                        try {
                            bufferJoinRelation.write(joinResult);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }

                }
                try {
                    innerTuple = bufferInnerRelation.readLine();
                    if(innerTuple!=null) innerTupleFields = innerTuple.split(",");
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            try {
                bufferInnerRelation.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
       
        try {
            bufferInnerRelation.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
              
    }
}
