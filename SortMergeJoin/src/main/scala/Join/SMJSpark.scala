package Join

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.io._
import java.nio.file.Files
import java.nio.file.Paths

object SMJSpark {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SortMergeJoin")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val inputPath1 = "tables/clients_heavy.csv"
    val inputPath2 = "tables/purchases_heavy.csv"

    val outputDir = "results"
    val outputFile = "joined.csv"
    val outputPath = outputDir + "/" + outputFile
    if (!Files.exists(Paths.get(outputDir))) Files.createDirectory(Paths.get(outputDir))

    val start = System.currentTimeMillis()

    val t1 = sc.textFile(inputPath1)
               .map(line => line.split(","))
               .map(record => (record(0).toInt, record))
    val t2 = sc.textFile(inputPath2)
               .map(line => line.split(","))
               .map(record => (record(0).toInt, record))

    val smj = new SortMergeJoin(t1, t2)
    val joined = smj.join("Hash", 4)

    val endJoin = System.currentTimeMillis()

    writeRDDToFile(joined, outputPath)
    val end = System.currentTimeMillis()

    val joinDuration = endJoin - start;
    val totalDuration = end - start;
    println(joined.count() + " records")
    println("Join Duration: " + joinDuration + " ms")
    println("Total Duration: " + totalDuration + " ms")
  }

  def writeRDDToFile(rdd: RDD[Array[String]], outputPath: String) = {
    val bw = new BufferedWriter(new FileWriter(outputPath))
    try {
      rdd.collect().foreach(record => bw.write(record.mkString(",") + "\n"))
    } finally bw.close()
  }
}




