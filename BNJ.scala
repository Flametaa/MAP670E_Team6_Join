import java.util
import org.apache.spark.{Sparkconf,SparkContext}
object BNJ {
  def main(args: util.Arrays[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Block_Nested_Join")
    val sc = new SparkContext(conf)

    val relation_R = sc.TestFile(path ="")
    val relation_S = sc.Testfile(path ="")

    split(relation_R)
    split(relation_S)
  }
  def split(RDD): Unit ={
    //split the relation database into pieces
    //For example, we have 2^n machine we can separate the databases in two n pieces
  }
  def Buffer_block(): Unit ={
    //simulate the block

  }
  def Bl_join(condition,RDD1 RDD2,block_number): Unit ={
    //for 1->  block_number
      //read the data in to Buffer_block
      //using for loop to check if some tuple match the condition
  }

}
