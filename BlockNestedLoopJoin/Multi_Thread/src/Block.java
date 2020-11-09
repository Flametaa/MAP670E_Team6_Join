import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

public class Block {
    private BufferedReader bufferRelation   = null;
    private int numbTuplesByBlock           = 0;

    public Block(BufferedReader bufferRelation, int numbTuplesByBlock) {
        this.bufferRelation     = bufferRelation;
        this.numbTuplesByBlock  = numbTuplesByBlock;
    }

    /* This funcion take a number of tuples from the the buffer and return them in the "tuples" variable
     * It return a Boolean to say if the buffer is empty
     * This is a Synchronized method, who avoid the concurrent acces
     */
    synchronized public boolean loadBlock(ArrayList<Tuple> tuples) throws IOException {
        String tuple        = bufferRelation.readLine();
        int tupleNumber     = 0;
        boolean isEmpty     = false;

        tuples.clear();
        while (tuple != null && tupleNumber < numbTuplesByBlock ){
            tuples.add(new Tuple(tuple));
            tupleNumber++;
            if(tupleNumber < numbTuplesByBlock) tuple = bufferRelation.readLine();
            
        }

        if(tuple == null)
            isEmpty = true;
        
        return isEmpty;
    }
}
