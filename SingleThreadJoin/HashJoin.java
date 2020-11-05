import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.io.IOException;

public class HashJoin{
    //This is the simple Hash Join single thread implementation. 
    //It operates in two phases: Build, Probe/Join

    private String rPath;
    private String sPath;

    //Indices of the keys we are conditioning in each relation
    private int rKey;
    private int sKey;

    //This buffer is going to write directly into the result csv file of the join operation
    //As later on the file will be shared between threads, it is necessary to pass it as an arg
    private BufferedWriter resultBuffer;
    
    public HashJoin(String rPath, String sPath, int rKey, int sKey, long rSize, long sSize, BufferedWriter resultBuffer){
        //To avoid having another pass on both the datasets,
        //we assume here that their size is known, which will be the case in our Grace Join
        //For the sake of coherence, let R be the smallest dataset

        this.rPath = rPath; this.rKey = rKey;
        this.sPath = sPath; this.sKey = sKey;

        if (sSize<rSize){
            this.rPath = sPath; this.rKey = sKey;
            this.sPath = rPath; this.sKey = rKey;  
        } 

        this.resultBuffer = resultBuffer;
    }

    public Map<String, String> buildHashTable(){
        /*Builds a Hash table of the smallest dataset.
        /The keys of the hashMap are the keys for the join. The values are the rows.
        /Collision may occurr, i.e two entries end up in the same bucket
        /The HashMap implicitely deals with that using linked lists.*/
        
        Map<String, String> hmap = new HashMap<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(rPath))) {
            String rRow;
            while ((rRow = br.readLine()) != null){
                String key = rRow.split(",")[rKey];
                hmap.put(key, rRow);
            }
            return hmap;
        }
        catch (IOException e){ 
            System.err.format("IOException: %s%n", e);
        }
        return null;
    }

    public void join(){
        //Builds the HashMap for the smaller dataset
        Map<String, String> hmap = buildHashTable();

        //Scan the larger dataset and probe the table to satisfy our condition*/
        try (BufferedReader br = Files.newBufferedReader(Paths.get(sPath))){
            String sRow;
            while ((sRow = br.readLine()) != null){
                String key = sRow.split(",")[sKey];

                //rRow contains the mapping to key if it exists and if key = k
                //otherwise, is null
                String rRow = hmap.get(key);
                if (rRow != null){
                    try{
                        //Remove the duplicate key column before writing 
                        String[] sRowArray = sRow.split(",") ;
                        sRowArray = Arrays.stream(sRowArray).filter(s -> !s.equals(key)).toArray(String[]::new);
                        sRow = String.join(",", sRowArray);

                        resultBuffer.write(rRow + "," + sRow + "\n");
                    } catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        } catch(IOException e){System.err.format("IOException: %s%n", e);}
    }
}