import java.io.File;
import java.nio.file.Paths;


public class Main {
    public static void main(String[] args){
        String folderPath = new File("").getAbsolutePath(); //absolute path of the current directory
        System.out.println("Working directory: " + folderPath);

        //All data is to be stored in the data folder in the parent directory
        String dataPath = Paths.get(folderPath, "data").toString();
        System.out.println("Data directory: " + dataPath);
        System.out.println("All files will be saved here. \n");

        //Please make sure to pass the names with no extension
        //Select the databases to join from the data folder
        String rName = "clients";
        String sName= "purchases";

        //Indices of the keys we're conditioning in each dataset
        int rKey = 0;
        int sKey = 0;

        //number of partitions
        int n = 5;
        //Whether or not to delete the partition files after execution
        Boolean keepPartitions = false;

        //Result of the join will be stored in this file
        String resultPath = Paths.get(dataPath, "join_" + rName + "_" + sName + ".csv").toString(); //cross-platform
        FileManager outputFile = new FileManager(resultPath);
        outputFile.createFile();

        // Single-thread Grace Join
        System.out.println("------ Starting single-threaded Grace Join ------\n");
        //Benchmarking execution time
        long startTime = System.nanoTime();
        GraceJoin graceHash = new GraceJoin(rName, sName, dataPath, rKey, sKey, n, outputFile);
        graceHash.graceJoin();


        long stopTime = System.nanoTime();
        System.out.println("The total single-threaded execution time is :"+(stopTime-startTime)/1e9+" seconds\n");


        //Multi-threaded Grace Join
        System.out.println("------Starting multi-threaded Grace Join------\n");
        //Benchmarking execution time
        startTime = System.nanoTime();
        MultiGrace multiGraceHash = new MultiGrace(rName, sName, dataPath, rKey, sKey, n, outputFile);
        multiGraceHash.graceJoin();
      
        outputFile.closeFile();
      
        stopTime = System.nanoTime();
        System.out.println("The total multi-threaded execution time is :"+(stopTime-startTime)/1e9+" seconds\n");

        //Benchmarking Memory used
        int bytesPerMB = 1024 * 1024;
        System.out.println("Total Memory :" + Runtime.getRuntime().totalMemory()/bytesPerMB + "MB\n");
        System.out.println("Used Memory :  " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/bytesPerMB + "MB\n");

        //Deleting partition files if not wanted
        if (!keepPartitions){
            System.out.println("Deleting partitions..\n");

            File dataDirectory = new File(dataPath);
            for (File f : dataDirectory.listFiles()) {
                String fName = f.getName();
                if (fName.startsWith(rName + '_') || fName.startsWith(sName + '_')) {
                    f.delete();
                }
            }
        }
    }
}