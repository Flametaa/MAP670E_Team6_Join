import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

//Extendible hashing Class

public class EHash {
    public int globalDepth;
    public Map<Integer,Integer> localDepths;
    //files tree
    public Map<Integer,FileManager> files;
    //length of each file
    Map<Integer,Integer> lengths;
    //Maximum number of records per file
    public int maxR;
    //path of all files
    public String path;
    //the join key id
    public int id;

    public EHash(int globalDepth,int maxR,String dataPath,int id){
        this.globalDepth = globalDepth;
        this.maxR = maxR;
        this.id=id;
        this.path = dataPath;
        localDepths = new HashMap<>();
        files = new HashMap<>();
        lengths = new HashMap<>();
        //initialize localdepth to globaldebth and create all corresponding files
        //initialize lengths to 0
        // Used on the first DB
        for (int i=0;i<(1<<this.globalDepth);i++){
            localDepths.put(i,globalDepth);
            FileManager file = new FileManager(this.path + "_" + i +"_" + localDepths.get(i)+ ".csv");
            file.createFile();
            files.put(i,file);
            lengths.put(i,0);
        }

    }

    // Second constructor fot building the hashed partitions from a given localDepth map
    // no need to redo the Ehashing process since we want the hash function of both databases to be the same
    // Used for the second database

    public EHash(int globalDepth,int maxR,String dataPath,int id,Map<Integer,Integer> localDepths){
        this.globalDepth = globalDepth;
        this.maxR = maxR;
        this.id=id;
        this.path = dataPath;
        this.localDepths = localDepths;
        files = new HashMap<>();
        lengths = new HashMap<>();

        for (int i=0;i<(1<<this.globalDepth);i++){
            if (localDepths.containsKey(i)){
                FileManager file = new FileManager(this.path + "_" + i +"_" + localDepths.get(i)+ ".csv");
                file.createFile();
                files.put(i,file);
                lengths.put(i,0);
            }

        }

    }

    // Get the join key from the dbRow and the id
    public String getKey(String line){
        String key="";
        int temp=0;
        for (int i=0;i<line.length();i++){
            if (line.charAt(i)==',') temp++;
            if (temp>id) break;
            if (temp==id && line.charAt(i)!=',') key+=line.charAt(i);


        }

        return key;
    }

    // Get the hashCode of the key
    public int getHash(String key){
        // String hashCode
        int hash = Math.abs(key.hashCode());
        // Take the rightMost globalDepth bits
        int rightMostBits = hash&((1<<(this.globalDepth))-1);
        // if localDepths[rightMostBits] exist then our localDepth=globalDepth
        // Otherwise we should look for localDepth by extracting 1 from globalDepth every time

        int count=1;
        while (!localDepths.containsKey(rightMostBits)){
            rightMostBits = hash&((1<<(this.globalDepth-count))-1);
            count++;
        }
        return rightMostBits;
    }


    // Used with second constructor
    // we will use already filled LocalDepth map in order to hash the second database into smaller files
    public void partitionFromLocalDepth(){
        try (BufferedReader br = Files.newBufferedReader(Paths.get(this.path + ".csv"))) {
            String dbRow;
            // Traverse the database
            while ((dbRow = br.readLine()) != null) {

                String key = getKey(dbRow);
                // Get the hash of the key
                int rightMostBits = getHash(key);

                // if the file with the same hash exists for the other DB then writeO the record on file
                if (files.containsKey(rightMostBits)){
                    FileManager file = files.get(rightMostBits);
                    file.writeOnFile(dbRow+"\n");
                    lengths.put(rightMostBits,lengths.get(rightMostBits)+1);
                }



            }
        } catch (IOException e) {System.err.format("IOException: %s%n", e);}



    }


    // Method to add record to the Tree of directories
    // Used for the first Database
    public void add(String line){
        String key = getKey(line);
        int rightMostBits = getHash(key);
        // If by adding the new record we will not depass the max records per file then we just write on the selected file

        if (lengths.get(rightMostBits)<maxR){
            FileManager file = files.get(rightMostBits);
            file.writeOnFile(line+"\n");
            lengths.replace(rightMostBits,lengths.get(rightMostBits)+1);
        }
        // Else we need to split the current file corresponding to the hash
        else{
            // If the localDepth=GlobalDepth then we need to increment the globalDepth in order to split
            if (localDepths.get(rightMostBits)==this.globalDepth){
                this.globalDepth++;
            }
                // Divide the file into 2 files by adding another bit to the left (0 for hash0 and 1 for hash1)

                int hash0 = rightMostBits;
                int hash1 = (1<<(localDepths.get(hash0)))+hash0;

                // Increment local depth for both hashes

                localDepths.replace(hash0,localDepths.get(hash0)+1);
                localDepths.put(hash1,localDepths.get(hash0));

                // Create the new files
                FileManager file0 = new FileManager(this.path + "_" + hash0+"_" + localDepths.get(hash0)+".csv");
                FileManager file1 = new FileManager(this.path + "_" + hash1+"_" + localDepths.get(hash1)+".csv");

                // Initialize the length of both files
                lengths.put(hash0,0);
                lengths.put(hash1,0);

                file0.createFile();
                file1.createFile();

                // Flush the buffer of the old file (in case it isnt flushed

                files.get(hash0).flushBuffer();
                // Traverse the old file and split it in the twi new files
                try (BufferedReader br = Files.newBufferedReader(Paths.get(this.path +"_" + (hash0) +"_"+ (localDepths.get(hash0)-1)+ ".csv"))) {
                    String dbRow;

                    while ((dbRow = br.readLine()) != null) {
                        String key_ = getKey(dbRow);
                        int hash_ = key_.hashCode();
                        int rightMostBits_ = hash_&((1<<(localDepths.get(hash0)))-1);
                        if (rightMostBits_==hash0) {
                            file0.writeOnFile(dbRow+"\n");
                            lengths.put(hash0,lengths.get(hash0)+1);
                        }
                        else{
                            file1.writeOnFile(dbRow+"\n");
                            lengths.put(hash1,lengths.get(hash1)+1);

                        }

                    }
                } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                // Delete the old file
                files.get(hash0).closeFile();
                files.get(hash0).deleteFile();
                // Put the new files in our file map
                files.put(hash0,file0);
                files.put(hash1,file1);



        }










    }


}
