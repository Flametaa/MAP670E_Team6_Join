import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;



public class EHash {
    public int globalDepth;
    public Map<Integer,Integer> localDepths;
    public Map<Integer,FileManager> files;
    Map<Integer,Integer> lengths;
    public int maxR;
    public String path;
    public int id;

    public EHash(int globalDepth,int maxR,String dataPath,int id){
        this.globalDepth = globalDepth;
        this.maxR = maxR;
        this.id=id;
        this.path = dataPath;
        localDepths = new HashMap<>();
        files = new HashMap<>();
        lengths = new HashMap<>();

        for (int i=0;i<(1<<this.globalDepth);i++){
            localDepths.put(i,globalDepth);
             FileManager file = new FileManager(this.path + "_" + i +"_" + localDepths.get(i)+ ".csv");
            file.createFile();
            files.put(i,file);
            lengths.put(i,0);
        }

    }
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
    public int getHash(String key){

        int hash = Math.abs(key.hashCode());
        int rightMostBits = hash&((1<<(this.globalDepth))-1);
        int count=1;
        while (!localDepths.containsKey(rightMostBits)){
            rightMostBits = hash&((1<<(this.globalDepth-count))-1);
            count++;
        }
        return rightMostBits;
    }

    public void partitionFromLocalDepth(){
        try (BufferedReader br = Files.newBufferedReader(Paths.get(this.path + ".csv"))) {
            String dbRow;

            while ((dbRow = br.readLine()) != null) {
                String key = getKey(dbRow);
                int rightMostBits = getHash(key);
                if (files.containsKey(rightMostBits)){
                    FileManager file = files.get(rightMostBits);
                    file.writeOnFile(dbRow+"\n");
                    lengths.put(rightMostBits,lengths.get(rightMostBits)+1);
                }



            }
        } catch (IOException e) {System.err.format("IOException: %s%n", e);}



    }

    public void add(String line){
        String key = getKey(line);
        int rightMostBits = getHash(key);

        if (lengths.get(rightMostBits)<maxR){
            FileManager file = files.get(rightMostBits);
            file.writeOnFile(line+"\n");
            lengths.replace(rightMostBits,lengths.get(rightMostBits)+1);
        }
        else{
            if (localDepths.get(rightMostBits)==this.globalDepth){
                this.globalDepth++;
            }

                int hash0 = rightMostBits;
                int hash1 = (1<<(localDepths.get(hash0)))+hash0;

                localDepths.replace(hash0,localDepths.get(hash0)+1);
                localDepths.put(hash1,localDepths.get(hash0));

                FileManager file0 = new FileManager(this.path + "_" + hash0+"_" + localDepths.get(hash0)+".csv");
                FileManager file1 = new FileManager(this.path + "_" + hash1+"_" + localDepths.get(hash1)+".csv");

                //System.out.println("OLD= "+hash0 +"_"+ (localDepths.get(hash0)-1)+"        NEW = "+ hash1 +"_" +(localDepths.get(hash0)) );
                lengths.put(hash0,0);
                lengths.put(hash1,0);

                file0.createFile();
                file1.createFile();


                files.get(hash0).flushBuffer();
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
                files.get(hash0).closeFile();
                files.get(hash0).deleteFile();

                files.put(hash0,file0);
                files.put(hash1,file1);



        }










    }


}
