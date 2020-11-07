import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.util.Map;

import java.io.IOException;

import static java.lang.Math.abs;
public class Partitioner extends  Thread{
    /* 
    *
    */
    String dbName;
    String dataPath;
    int id;
    int n;
    Map<String, long[]> partitionLengths;

    public Partitioner(String dbName, String dataPath, int id, int n, Map<String, long[]> partitionLengths){
        this.dbName = dbName;
        this.dataPath = dataPath;
        this.id = id;
        this.n = n;
        this.partitionLengths = partitionLengths;
    }

    @Override
    public void run(){

        System.out.println("Partitioner started..\n");
        //Contructs the path for the data to be stored
        String filesPath = Paths.get(dataPath, dbName).toString(); 
        //Allocates buffer writers
        BufferedWriter[] buffers = new BufferedWriter[n+1];

        //Creating n files
        for (int i = 0; i < n; i++){
            File file = new File(filesPath + "_" + i + ".csv"); //Careful, dbName has to be without extension
            if (!file.exists()) {
                try {file.createNewFile();} catch (IOException e) {e.printStackTrace();}
            }
            //For each file, create a BufferWriter
            FileWriter fw = null;
            try { fw = new FileWriter(file);} 
            catch (IOException e) {e.printStackTrace();}

            buffers[i] = new BufferedWriter(fw);
        }

        //We scan the data set and hash the keys of each row.
        //The rows with the same hashCode end up in the same bucket, i.e, same partition
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filesPath + ".csv"))) {
            String dbRow;
            while ((dbRow = br.readLine()) != null) {

                String key = dbRow.split(",")[id];
                int hashCode = abs(key.hashCode()) % n;

                //The row falls into the bucket of index hashCode
                buffers[hashCode].write(dbRow + "\n"); //no need to flush
   
                //Increment the length of partition of index hashCode
                partitionLengths.get(dbName)[hashCode]++;
 
            }
        } catch (IOException e) {System.err.format("IOException: %s%n", e);}

        //Close all files after partitioning
        for (int i = 0; i < n; i++){
            try { buffers[i].close();} 
            catch (IOException e) {e.printStackTrace();}
        }

    }
}
