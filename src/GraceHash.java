import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

import static java.lang.Math.E;
import static java.lang.Math.abs;

public class GraceHash {
    //This is the Grace Join single thread implementation.
    //It proceeds the simple algorithm by a partitioning phase.
    //The partitioning is done on both datasets to finally get R1,..Rn and S1,..Sn
    //The classic algorithm HashJoin is then called for each pair of partitions (Ri, Si)

    private String dataPath;
    private String rName;
    private String sName;
    private int rKey;
    private int sKey;
    private FileManager outputFile;

    //The number of partitions is to be tuned by the user and is passed on as an arg.
    private int n;

    //We recover the size of each partition during the partitioning phase.
    //This is done to avoid doing another scan over both datasets later on.
    //These lengths will be passed to the classic HashJoin algorithm when called for each pair (Ri, Si)
    private Map<String, long[]> partitionLengths;

    public GraceHash(String rName, String sName, String dataPath, int rKey, int sKey, int n, FileManager outputFile){
        this.dataPath = dataPath;
        this.rName = rName;
        this.sName = sName;
        this.rKey = rKey;
        this.sKey = sKey;
        this.n = n;
        this.outputFile = outputFile;


        //Initialising the lengths to zeros (rName, [0,0,..]) and (sName, [0,0,..])
        this.partitionLengths = new HashMap<>();

        this.partitionLengths.put(rName, new long[n]);
        Arrays.fill(this.partitionLengths.get(rName), 0);
        this.partitionLengths.put(sName, new long[n]);
        Arrays.fill(this.partitionLengths.get(sName), 0);

    }

    public void partition(String dbName, int id){
        //Splits the database of name "name" into n partitions.
        //It creates n files (dbName0, dbName1,..) in folderPath.
        //This is done using a hash function on the join key. The bucket a row falls into, is the hash of its key.
        //It is different from the hash funciton used for mapping the larger dataset in the classic join algorithm.

        String filesPath = Paths.get(dataPath, dbName).toString();  //Base name for files, without extention



        //As we are going to create n files, we need n BufferWriters
        FileManager[] buffers = new FileManager[n+1];

        //Creating n files
        for (int i = 0; i < n; i++){
            FileManager file = new FileManager(filesPath + "_" + i + ".csv");
            file.createFile();
            buffers[i] = file;
        }

        //We scan the data set and hash the keys of each row.
        //The rows with the same hashCode end up in the same bucket, i.e, same partition
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filesPath + ".csv"))) {
            String dbRow;
            boolean rec = true;
            while ((dbRow = br.readLine()) != null) {
                /*if (rec){
                    Record record = new Record(dbRow,dbName);
                    int idx = record.keyToId[id];
                    rec = false;
                    continue;
                }*/
                String key = dbRow.split(",")[id];
                int hashCode = abs(key.hashCode()) % n;

                //The row falls into the bucket of index hashCode
                buffers[hashCode].writeOnFile(dbRow + "\n"); //no need to flush

                //Increment the length of partition of index hashCode
                partitionLengths.get(dbName)[hashCode]++;

            }
        } catch (IOException e) {System.err.format("IOException: %s%n", e);}

        //Close all files after partitioning
        for (int i = 0; i < n; i++){
            buffers[i].closeFile();
        }
    }

    public Map<Integer,Integer> partition1(){
        //Splits the database of name "name" into n partitions.
        //It creates n files (dbName0, dbName1,..) in folderPath.
        //This is done using a hash function on the join key. The bucket a row falls into, is the hash of its key.
        //It is different from the hash funciton used for mapping the larger dataset in the classic join algorithm.

        String filesPath = Paths.get(dataPath, rName).toString();  //Base name for files, without extention

        EHash eHashR = new EHash(1,5000,filesPath,rKey);

        //We scan the data set and hash the keys of each row.
        //The rows with the same hashCode end up in the same bucket, i.e, same partition
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filesPath + ".csv"))) {
            String dbRow;

            while ((dbRow = br.readLine()) != null) {
                eHashR.add(dbRow);
            }
        } catch (IOException e) {System.err.format("IOException: %s%n", e);}

        eHashR.files.forEach((k, v) -> v.closeFile() );
        String filesPath1 = Paths.get(dataPath, sName).toString();  //Base name for files, without extention

        EHash eHashS = new EHash(eHashR.globalDepth,5000,filesPath1,sKey,eHashR.localDepths);
        eHashS.partitionFromLocalDepth();
        eHashS.files.forEach((k, v) -> v.closeFile() );
        n = eHashR.globalDepth;
        /*
        this.partitionLengths.put(rName, new long[1<<n]);
        Arrays.fill(this.partitionLengths.get(rName), 0);
        this.partitionLengths.put(sName, new long[1<<n]);
        Arrays.fill(this.partitionLengths.get(sName), 0);

        eHashR.lengths.forEach((k,v)->partitionLengths.get(rName)[k]=v);
        eHashS.lengths.forEach((k,v)->partitionLengths.get(sName)[k]=v);

        */
        return eHashR.localDepths;

    }

    public void graceJoin(){

        //partition both datasets

        partition(rName, rKey);
        partition(sName, sKey);

        //Now we need to call the classic join on each pair
        for (int i = 0; i < n; i++){

            //construct the appropriate path
            String rPath = Paths.get(dataPath, rName + "_" + i + ".csv").toString();
            String sPath = Paths.get(dataPath, sName + "_" + i + ".csv").toString();


            //Retrive the lengths of each partition
            long rSize = partitionLengths.get(rName)[i];
            long sSize = partitionLengths.get(sName)[i];
            if (rSize == 0 || sSize == 0) continue;

            HashJoin joinPartition = new HashJoin(rPath,sPath, rKey, sKey, rSize, sSize, outputFile);
            joinPartition.join();

        }
    }

    //Join Using Extendible Hash
    public void graceJoinEHash(){


        Map<Integer,Integer> localDepths = partition1();
        //Now we need to call the classic join on each pair
        for (int i = 0; i < (1<<n); i++){

            //construct the appropriate path
            if (localDepths.containsKey(i)){
                String rPath = Paths.get(dataPath, rName + "_" + i +"_" + localDepths.get(i)+ ".csv").toString();
                String sPath = Paths.get(dataPath, sName + "_" + i +"_" + localDepths.get(i)+ ".csv").toString();


                HashJoin joinPartition = new HashJoin(rPath,sPath, rKey, sKey,outputFile);
                joinPartition.join();
            }


        }
    }
}
