import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE
import org.apache.spark.{SparkConf, SparkContext}


object main {
  def main(arg: Array[Int]): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("Block_Nested_Join")
    val sc = new SparkContext(conf)

    val relation_R = sc.textFile(path = "data1")
    val relation_S = sc.textFile(path = "data2")

    var BlockSize: Int = 10
    var Block: BNJ = (relation_R,relation_S,BlockSize)
    val joinRDD = Block
    sc.stop()
  }
}
