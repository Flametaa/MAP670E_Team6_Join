package Join

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.nio.file.Paths
import java.nio.file.Files

object SMJSpark {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SortMergeJoin")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val inputPath1 = "tables/clients_heavy.csv"
    val inputPath2 = "tables/purchases_heavy.csv"

    val tempDir = "temp"
    val outputDir = "results"
    if (!Files.exists(Paths.get(outputDir))) Files.createDirectory(Paths.get(outputDir))
    
    val outputPath = "results/joined.csv"

    val start = System.currentTimeMillis()

    val t1 = sc.textFile(inputPath1)
               .map(line => line.split(","))
               .map(record => (record(0).toInt, record))
    val t2 = sc.textFile(inputPath2)
               .map(line => line.split(","))
               .map(record => (record(0).toInt, record))

    val smj = new SortMergeJoin(t1, t2)
    val joined = smj.join("Hash", 5)
    
    val endJoin = System.currentTimeMillis()

    FileManager.writeRDDToFile(joined, tempDir, outputDir, outputPath)
    val end = System.currentTimeMillis()

    val joinDuration = endJoin - start;
    val totalDuration = end - start;
    
    println(joined.count() + " records")
    println("Sort-Merge Join Duration: " + joinDuration + " ms")
    println("Total Duration: " + totalDuration + " ms")
    
    println("\nCleaning Disk...");
    FileManager.delete(tempDir);
    
    sc.stop();
  }
}




