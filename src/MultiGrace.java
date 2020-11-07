import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class MultiGrace {
    /*
    *This is the Grace Join single thread implementation. 
    *It proceeds the simple algorithm by a partitioning phase.
    *The partitioning is done on both datasets to finally get R1,..Rn and S1,..Sn
    *The classic algorithm HashJoin is then called for each pair of partitions (Ri, Si)
    */
    private String dataPath;
    private String rName;
    private String sName;
    private int rKey;
    private int sKey;
    private  FileManager outputFile;

    //The number of partitions is to be tuned by the user and is passed on as an arg.
    private int n;

    /*
    *We recover the size of each partition during the partitioning phase.
    *This is done to avoid doing another scan over both datasets later on.
    *These lengths will be passed to the classic HashJoin algorithm when called for each pair (Ri, Si)
    */
    private Map<String, long[]> partitionLengths;

    public MultiGrace(String rName, String sName, String dataPath, int rKey, int sKey, int n, FileManager outputFile){
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

    public void graceJoin(){
        long startTime = System.nanoTime();

        //partition both datasets
        Partitioner rPartitioner = new Partitioner(rName, dataPath, rKey, n, partitionLengths);
        rPartitioner.run();

        Partitioner sPartitioner = new Partitioner(sName, dataPath, sKey, n, partitionLengths);
        sPartitioner.run();

        /*
        *Before moving to the parallel joins, we have to make sure both partitioning threads are done
        *We can't start joining before this happens, because we don't know when a partition is full.
        *This is due to the use of hash code.
        */
        try {rPartitioner.join();} 
        catch (InterruptedException e) {e.printStackTrace();}
        System.out.println("Partitioner ended..\n");

        try {sPartitioner.join();} 
        catch (InterruptedException e) {e.printStackTrace();}
        System.out.println("Partitioner ended..\n");


        long stopTime = System.nanoTime();
        System.out.println("Parallel partitioning took: "+(stopTime-startTime)/1e9+" seconds\n");

        /*
        *The obvious approach would be to have n threads, i.e one thread per partition.
        *This way, in the worst case scenario, we owuld have to store n hashmaps in memory
        *Which would be against the whole purpose of The Grace Algorithm.
        *
        *The idea is to have a pool of Threads, that can be reused once done executing.
        *The number of the threads in the pool here is fixed to the number of CPU cores
        */
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("Cores: " + cores);
        ExecutorService executor = Executors.newFixedThreadPool(cores); // Thread pool

        startTime = System.nanoTime();

        //Now we need to call the classic join on each pair 
        for (int i = 0; i < n; i++){
            //construct the appropriate path
            String rPath = Paths.get(dataPath, rName + "_" + i + ".csv").toString();
            String sPath = Paths.get(dataPath, sName + "_" + i + ".csv").toString();

            //Retrieve the lengths of each partition
            long rSize = partitionLengths.get(rName)[i];
            long sSize = partitionLengths.get(sName)[i];
            if (rSize == 0 || sSize == 0) continue;

            MultiJoin joinPartition = new MultiJoin(rPath,sPath, rKey, sKey, rSize, sSize, outputFile); //Runnable
            // Add the thread to the pool
            executor.execute(joinPartition);
        }

        // No new tasks will be accepted. Previously launched tasks will still finish their execuion
        executor.shutdown(); 

        try {
            // blocks until all tasks have completed execution after the shutdown request
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); 
        } catch (InterruptedException e) {
            System.out.println(":(");
        }


        stopTime = System.nanoTime();
        System.out.println("Parallel joining took: "+(stopTime-startTime)/1e9+" seconds\n");
    }
}
