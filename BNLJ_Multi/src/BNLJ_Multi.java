import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BNLJ_Multi {
    public static void main(String[] args) throws Exception {
        /*  Tables  */
        String outerTableFile               = "./data/authors.csv";
        String innerTableFile               = "./data/posts.csv";
        String joinTableFile                = "./data/results.csv";

        /** Variables   **/
        FileReader dataFileInnerRelation    = null;
        FileReader dataFileOuterRelation    = null;
        FileWriter dataFileJoinRelation     = null;
        BufferedReader bufferOuterRelation  = null;
        BufferedReader bufferInnerRelation  = null;
        BufferedWriter bufferJoinRelation   = null;
        int blockSize                       = 64;
        int bufferSize                      = 1024*blockSize;
        ArrayList<String> innerBlock        = new ArrayList<String>();
        ArrayList<String> outerBlock        = new ArrayList<String>();
        boolean isEmptyBufferOuterRelation  = false;
        boolean isEmptyBufferInnerRelation  = false;
        long startTime                      = System.nanoTime();
        long stopTime                       = 0;

        try{
            dataFileOuterRelation   = new FileReader(outerTableFile);
            dataFileJoinRelation    = new FileWriter(joinTableFile );
            bufferOuterRelation     = new BufferedReader(dataFileOuterRelation,bufferSize);
            bufferJoinRelation      = new BufferedWriter(dataFileJoinRelation );

            while(!isEmptyBufferOuterRelation){
                isEmptyBufferOuterRelation  = loadBlock(bufferOuterRelation,outerBlock,blockSize);
                dataFileInnerRelation       = new FileReader(innerTableFile );
                bufferInnerRelation         = new BufferedReader(dataFileInnerRelation,bufferSize);
                isEmptyBufferInnerRelation  = false;

                while(!isEmptyBufferInnerRelation){
                    isEmptyBufferInnerRelation = loadBlock(bufferInnerRelation,innerBlock,blockSize);

                    for (String outerTuple : outerBlock){
                        for(String innerTuple : innerBlock){
                            String[] outerTupleFields = outerTuple.split(",");
                            String[] innerTupleFields = innerTuple.split(",");

                            if(outerTupleFields[0].equals(innerTupleFields[0])){
                                String joinResult = String.join(",",outerTupleFields[0],outerTupleFields[1],innerTupleFields[2],"\n");
                                bufferJoinRelation.write(joinResult);
                            }
                        }
                    }
                }

                if(bufferInnerRelation!=null)   bufferInnerRelation.close();
                if(dataFileInnerRelation!=null) dataFileInnerRelation.close();
            }

            //String st = br.lines().collect(Collectors.joining("\n"));
        }
        catch(IOException e){
            System.out.println("Could not read the file...");
            e.printStackTrace();
        }
        finally{
            if(bufferOuterRelation!=null)   bufferOuterRelation.close();
            if(bufferInnerRelation!=null)   bufferInnerRelation.close();
            if(bufferJoinRelation!=null)    bufferJoinRelation.close();
            if(dataFileOuterRelation!=null) dataFileOuterRelation.close();
            if(dataFileInnerRelation!=null) dataFileInnerRelation.close();
            if(dataFileJoinRelation!=null)  dataFileJoinRelation.close();
        }
      
        stopTime    = System.nanoTime();
        System.out.println("Time: " + TimeUnit.SECONDS.convert(stopTime-startTime,TimeUnit.NANOSECONDS));
    }

    public static boolean loadBlock(BufferedReader buffer, ArrayList<String> block, int blockSize) throws IOException {
        String tuple        = buffer.readLine();
        int tupleNumber     = 1;
        boolean isEmpty     = false;

        block.clear();
        while (tuple != null && tupleNumber <= blockSize){
            block.add(tuple);
            if(tupleNumber < blockSize) tuple = buffer.readLine();
            tupleNumber++;
        }

        if(tuple == null)
            isEmpty = true;
        
        return isEmpty;
    }
}
