import java.io.BufferedWriter;
import java.io.IOException;

public class JoinOperation {

    /* Set the differents kinds of Querys Join*/
    public static void exec(Tuple outerTuple, Tuple innerTuple, BufferedWriter bufferJoinRelation, int i, int c1, int c2){
        
        String joinResult = null;
        switch (i){
            case 1  :   if(outerTuple.getField(c1).equals(innerTuple.getField(c2)) ) 
                            joinResult = String.join(",",outerTuple.getField(0),innerTuple.getField(0),innerTuple.getField(1),"\n");
                        break;
            case 2  :   if(outerTuple.getField(c1).equals(innerTuple.getField(c2)) ) 
                            joinResult = String.join(",",outerTuple.getTupleAsString(),innerTuple.getTupleAsString(),"\n");
                        break;
            case 3  :   if(outerTuple.getField(c1).equals(innerTuple.getField(c2)) ) 
                            joinResult = String.join(",",outerTuple.getField(0),innerTuple.getField(0), "\n");
                        break;
        }
        
        try {
            if(joinResult!=null)
                bufferJoinRelation.write(joinResult);
        } catch (IOException e) {
            e.printStackTrace();
        }
       
    }
}
