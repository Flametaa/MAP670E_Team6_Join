import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BNLJ_Multi {
    public static void main(String[] args) {
        // Tables 
        String outerTableFile = "./data/posts.csv";
        String innerTableFile = "./data/authors.csv";
        String joinTableFile = "./data/results.csv";

        // Variables 
        BufferedReader bufferOuterRelation = null;
        BufferedReader bufferInnerRelation = null;
        BufferedWriter bufferJoinRelation = null;
        int blockSize = 200;
        int bufferSize = 1024 * blockSize;
        ArrayList<String> innerBlock = new ArrayList<String>();
        ArrayList<String> outerBlock = new ArrayList<String>();
        boolean isEmptyBufferOuterRelation = false;
        long startTime = System.nanoTime();
        long stopTime = 0;

        try {
            bufferOuterRelation = new BufferedReader(new FileReader(outerTableFile));
            bufferJoinRelation = new BufferedWriter(new FileWriter(joinTableFile));
        } catch (IOException e) {
            System.out.println("Could not read the file...");
            e.printStackTrace();
        }

        while (!isEmptyBufferOuterRelation) {
            String innerTuple = null;
            String[] innerTupleFields = null;
            try {
                isEmptyBufferOuterRelation = loadBlock(bufferOuterRelation, outerBlock, blockSize);
                bufferInnerRelation = new BufferedReader(new FileReader(innerTableFile));
                innerTuple = bufferInnerRelation.readLine();
                innerTupleFields = innerTuple.split(",");
            } catch (FileNotFoundException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            //System.out.println(outerBlock.get(0).split(",")[0]);
            while(innerTuple != null){
                for (String outerTuple : outerBlock){
                    String outerTupleFields = outerTuple;
                    //String[] innerTupleFields = innerTuple.split(",");
                    
                    
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
            bufferOuterRelation.close();
            bufferJoinRelation.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        stopTime    = System.nanoTime();
        System.out.println("Time: " + TimeUnit.SECONDS.convert(stopTime-startTime,TimeUnit.NANOSECONDS));
    }

    public static boolean loadBlock(BufferedReader buffer, ArrayList<String> block, int blockSize) throws IOException {
        String tuple        = buffer.readLine();
        int tupleNumber     = 0;
        boolean isEmpty     = false;
        String [] attributes= null;

        block.clear();
        while (tuple != null && tupleNumber < blockSize){
            attributes = tuple.split(",");
            block.add(attributes[0]);
            tupleNumber++;
            if(tupleNumber < blockSize) tuple = buffer.readLine();
            
        }

        if(tuple == null)
            isEmpty = true;
        
        return isEmpty;
    }
}
