## Hash Join Implementation

This folder contains our implementations of the **Grace Join** algorithm. All data is found in the [data](/data) folder along with the resulting dataset. All the code is found in the [src](/src). It contains the single thread as well as the multithreaded implementation.

Running the [Main.java](/HashJoin_Clean/src/Main.java) would run both implementations on the existing datasets [clients.csv](/data/clients.csv) and [purchases.csv](/data/purchases.csv). It returns the result in [join_clients_purchases.csv](/data/join_clients_purchases.csv). It would also output the total execution time, the time it took to join and in case of multithreading, the joining the partioning time. 

(//TODO : for now just the multithread output --> add comparison, and Spark files)

If you wish to join different data sets, a few parameters will have to change in the [Main.java](/HashJoin_Clean/src/Main.java) file. Make sure to set the following : 

    - The name of the datasets with no extentions, no path needed.
    - The index of the keys for the equality condition
    - The number of partitions
    - If you want to keep the partitions, make sure to set keepPartitions = true.

- Single Thread Grace Join

    **Grace Hash Join** is a variation on the classic **Hash Join**, proceeding it by a partitioning phase of the two datasets. The following files ensure the implementation of the single thread Grace join.

    - [HashJoin.java](src/HashJoin.java) is the implementation of the simple Hash Join algorithm. It boils down to two stages and so contains two main functions responsible for: 

        - Building the hashmap for the smaller data set **R**, using a HashMap.
        - Scanning the larger dataset **S**, probing the Hashmap and checking the equality condition. It then writes the result to the output file.

    - [GraceJoin.java](/src/GraceJoin.java) implements Grace Join algorithm. It also operates in two steps using two dedicated functions to:
        - Partition both datasets into **n** partitions i.e *R1,.., Rn and S1,..,Sn*. This is done using a different hash function that'll decide which bucket each row would fall into.
        
        *Note: **n** is fixed by the user in the [Main.java](/HashJoin_Clean/src/Main.java)*
        - Iterates on each pair and calls the [HashJoin.java](src/HashJoin.java), the problem of joining two large tables is divided into m partial joins performed by a simple Hash Join each.

- Multi-thread implementation

    There are two things we can parallelize in this algorithm: The partitioning, and the joining. 

    - [Partitioner.java](/src/Partitioner.java) is a thread that'll take care of partitioning one dataset. Only two instances of this class are going to be needed. 
  
    - Parallel joins are ensured by the [MultiJoin.java](/src/MultiJoin.java) class and the [MultiGrace.java](/src/MultiGrace.java) class. The first one represents a runnable instance that'll take care of joining one pair of datasets. The second one ensures the partitioning is done and then executes multiple runnables to do the joining and create the resulting file, concurrently.

**Implementation details**

- **<ins>Partitioning</ins>**: Can we have more than two threads delaing with this task? No. As we will not know when a partition is completed until the whole dataset is scanned, we'll have to wait for both these threads to finish to start the joining. And we can't assign this task to more than two threads without changing the fundamentals behind the **Grace** algorithm.

- **<ins>Joining</ins>**: Our first approach was to associate one joining thread for each partition, for a total of **n** threads. However, this is fundamentally in contradiction with the Grace algorithm. The point is to avoid saving the hashtable of a large dataset in memory and work with one hashtable associated to one partition at a time. 

    Recall that each thread responsible for one join operation is going to build a hashmap of the smallest table in memory. In the worst case scenario, i.e, **n** threads running simultaneously (which can happen depending on **n** and the size of the initial tables), means we'll use as much space as a simple **Hash Join** does, storing the whole Hashmap in memory. 

    The idea here is to have a pool of threads. This is more efficient than the first approach in terms of memory. But it also slows the speed because we are limited by the size of our pool. Once all threads are currently executing, and we still need more partitions to join, i.e, **n** is greater than the number of **cores**, this task will be queued.

    **How to choose the number of threads?** 

    It didn't seem appropriate to have this parameter set by the user. It depends on the size of the original tables, n the number of partitions which is already manually tuned and on the hardware.
    
    So we opted to set the size of the pool to the number of **CPU cores**. In our pool, these runnables can be reused once they're done executing their task. It also has the advantage of allowing multiple threads to share the same object instance. 

- Optimization details

    - **<ins>Buffers</ins>**: We working with [BufferWriters](https://docs.oracle.com/javase/7/docs/api/java/io/BufferedWriter.html). These are thread safe when threads are trying to wirte to the same file. Which means we don't have to worry about locking the file, as the ````BufferWriter.write```` is synchronized and already has a **lock**. This blocking is slow but necessary to have correct output. 

    - **<ins>Flushing</ins>**: What we can do is wait for the buffer to be full and for it to flush on it's own, instead of flushing after every row that is ready to be written to the result file. To make this even faster, we save multiple rows before passing them to the buffer, which would accelerate the process because flushing is time consuming.



