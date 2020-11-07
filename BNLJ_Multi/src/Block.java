import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

public class Block {
    private BufferedReader bufferRelation = null;
    private int numbTuples = 0;

    public Block(BufferedReader br, int numbTuples) {
        this.bufferRelation = br;
        this.numbTuples = numbTuples;
    }

    synchronized public boolean loadBlock(ArrayList<String> block) throws IOException {
        String tuple        = bufferRelation.readLine();
        int tupleNumber     = 0;
        boolean isEmpty     = false;
        String [] attributes= null;

        block.clear();
        while (tuple != null && tupleNumber < numbTuples ){
            attributes = tuple.split(",");
            block.add(attributes[0]);
            tupleNumber++;
            if(tupleNumber < numbTuples) tuple = bufferRelation.readLine();
            
        }

        if(tuple == null)
            isEmpty = true;
        
        return isEmpty;
    }
}
