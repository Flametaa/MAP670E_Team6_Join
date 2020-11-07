import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.io.IOException;
import java.util.stream.Collectors;

public class MultiJoin implements  Runnable {
    /*
    *This is the modified simple join implementation
    *It implements runnable and represents a thread that would take care of joining a pair or datasets
    *It operates in two phases: Build, Probe/Join
    */

    private String rPath;
    private String sPath;

    //Indices of the keys we are conditioning in each relation
    private int rKey;
    private int sKey;

    //This buffer is going to write directly into the result csv file of the join operation
    //As later on the file will be shared between threads, it is necessary to pass it as an arg
    private FileManager outputFile;
    
    public MultiJoin(String rPath, String sPath, int rKey, int sKey, long rSize, long sSize, FileManager outputFile){
        //To avoid having another pass on both the datasets,
        //we assume here that their size is known, which will be the case in our Grace Join
        //For the sake of coherence, let R be the smallest dataset

        this.rPath = rPath; this.rKey = rKey;
        this.sPath = sPath; this.sKey = sKey;

        if (sSize<rSize){
            this.rPath = sPath; this.rKey = sKey;
            this.sPath = rPath; this.sKey = rKey;  
        } 

        this.outputFile = outputFile;
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

    @Override
    public void run(){
        //long runStartTime = System.nanoTime();

        //long startTime = System.nanoTime();
        //Builds the HashMap for the smaller dataset
        Map<String, String> hmap = buildHashTable();
        //long stopTime = System.nanoTime();

        //System.out.println("Time building hashtable: "+(stopTime - startTime)/1e9+" seconds\n");

        //long totalWriteTime = 0;

        // Store a number of lines in memory before giving it to the buffer
        int maxBufferedLines = 10;
        ArrayList<String> currentlyBufferedLines = new ArrayList<>();

        //Scan the larger dataset and probe the table to satisfy our condition*/
        try (BufferedReader br = Files.newBufferedReader(Paths.get(sPath))){
            String sRow;
            while ((sRow = br.readLine()) != null){
                String[] sRowArray = sRow.split(",") ;
                String key = sRowArray[sKey];

                //rRow contains the mapping to key if it exists and if key = k
                //otherwise, is null
                String rRow = hmap.get(key);
                if (rRow != null){
                   //Remove the duplicate key column before writing
                   sRow = Arrays.stream(sRowArray).filter(s -> !s.equals(key)).collect(Collectors.joining(","));
                   //sRow = String.join(",", sRowArray);

                   String line = rRow + "," + sRow + "\n";
                   currentlyBufferedLines.add(line);

                   // Reached the max? Flush it.
                   if (currentlyBufferedLines.size() == maxBufferedLines) {
                       String buf = currentlyBufferedLines.stream().collect(Collectors.joining("\n"));
                       //startTime = System.nanoTime();
                       outputFile.writeOnFile(buf);
                       //stopTime = System.nanoTime();
                       //totalWriteTime += (stopTime - startTime);

                       currentlyBufferedLines.clear();
                   }

                }
            }

            // Flush at the very end.
            if (currentlyBufferedLines.size() > 0) {
                String buf = currentlyBufferedLines.stream().collect(Collectors.joining("\n"));
                outputFile.writeOnFile(buf);
                currentlyBufferedLines.clear();
            }

        } catch(IOException e){System.err.format("IOException: %s%n", e);}
        
        //The total time is took for one pair of partitions to be joined
        //long runStopTime = System.nanoTime();
        //System.out.println("Total time spent writing: "+totalWriteTime/1e9+" seconds\n");
        //System.out.println("Total time spent in run(): "+(runStopTime - runStartTime)/1e9+" seconds\n");
    }
}