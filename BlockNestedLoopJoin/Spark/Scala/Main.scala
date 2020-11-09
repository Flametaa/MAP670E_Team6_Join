import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Block_Nested_Join")
    val sc = new SparkContext(conf)

    // Calculate Join time accuracy: millisecond
    val start = System.currentTimeMillis()

    // upload file into RDD
    val relation_R = sc.textFile(path = "./src/main/scala/authors.csv")
    val relation_S = sc.textFile(path = "./src/main/scala/posts.csv")
    println("number of the tuples(Outer relation):" + relation_R.count)
    println("number of the tuples(Inner relation):" + relation_S.count)

    val t1 = relation_R.map(line => line.split(",")).map(pairs => (pairs(0), pairs))
    val t2 = relation_S.map(line => line.split(",")).map(pairs => (pairs(0), pairs))

    // DO the Blocked nested join
    val bnlj = new BNLJ(t1, t2)
    val joined = bnlj.join()
    val end = System.currentTimeMillis()


    val TimeJoin = end - start;
    val t3 = t1.join(t2)
    println("Time for join: " + TimeJoin + " ms")
    println(t3.count())
    println("number of tuples(Result):" + joined.count())

    sc.stop()
  }


}