package org.spark.Team6Join

import org.apache.spark.SparkContext
import java.io._
import java.util._
import java.nio.file.Files
import java.nio.file.Paths


class GraceHashJoinSpark( private val sc:SparkContext,
                    private var pathDB1: String,
                    private var pathDB2: String,
                    private var outputPath: String,
                    private var id1: Int,
                    private var id2 : Int,
                    private var N: Int) {
  
  def partition( DB:String,id:Int,N:Int){
        var output=Array.fill(N+1)("")
        var dbPath:String=null
        if (DB=="R") dbPath = pathDB1;
        else dbPath = pathDB2;
        try {
          var dbfile=sc.textFile(dbPath) //We read the databse
          var splitRdd=dbfile.map(line => line.split("/n")).collect()
              splitRdd.foreach(x=>            
                {
                        val line=x(0) 
                        val key=line.split(',')(id)
                        val hash = (key.hashCode()%N).abs  //We apply the first hash for partition
                        output(hash)+=(line+"\n")
                 })


           
        }  
        
        catch {    case x: IOException   => 
        { 
            // Displays this if input/output  
            // exception is found 
            println("Input/output Exception in GraceHashJoin.scala/partition") 
          }  }
        
       for (i<-0 to N-1){
       var file = new File(DB+i+".txt")
       var fw = new FileWriter(file);
       var bufferedWriter = new BufferedWriter(fw)  //Here we read the resulted partition into local disk and we delete them after execution
       bufferedWriter.write(output(i))
       bufferedWriter.flush()
       bufferedWriter.close()
    }

             

    




  }
  
  def GraceJoin() {
    this.partition("R",id1,N)
    println("partition R Okey")
    this.partition("S",id2,N)
    println ("Partiton S Okey")
    var join_result=""
    
           for (i<-0 to N-1){
             var path1="R"+i+".txt"
             var path2="S"+i+".txt"
             var joinPartition = new ClassicHashJoinSpark(sc,path1,path2,id1,id2)
             join_result+=joinPartition.join() //We apply a simple hash join for each partitions number i
             var fileR = new File("R"+i+".txt")
             var fileS = new File("S"+i+".txt")
              if (fileR.exists() & fileS.exists()){ //Deleting partition files. Delete this part if you want to let the partition.
                 fileR.delete()
                 fileS.delete()
               }  
           }
           var outputRDD= sc.parallelize(Seq(join_result))
           outputRDD.saveAsTextFile(outputPath+".csv")

  }
}
