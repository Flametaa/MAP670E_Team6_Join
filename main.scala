import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object main {
  def main(): Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("Block_Nested_Join")
    val sc = new SparkContext(conf)

    val relation_R = sc.textFile(path = "data1")
    val relation_S = sc.textFile(path = "data2")
    val arrayR = relation_R.collect()
    val arrayS = relation_S.collect()
    var arrayO: ArrayBuffer[String] = new ArrayBuffer[String]


    // Define a Block
    val BlockSize: Int = 10
    val TuplePerPage: Int = 1000
    val Block: BufferBlock = new BufferBlock(TuplePerPage,BlockSize)
    // Put dataset tuple into block
    // For
    var times1: Int = 1
    var times2: Int = 1
    while (times1*TuplePerPage*(BlockSize-2) <= arrayR.length){
      Block.ReadR(arrayR.slice((times1-1)*TuplePerPage*(BlockSize-2),times1*TuplePerPage*(BlockSize-2)))
      times1+=1
      //Input  Inner relation par page and do the join
      while (times2*TuplePerPage <= arrayS.length){
        Block.ReadS(arrayS.slice((times2-1)*TuplePerPage,times2*TuplePerPage))
        times2+=1
        join(sc.parallelize(Block.BlockContentR), sc.parallelize(Block.BlockContentS))
      }
      Block.ReadS(arrayS.slice((times2-1)*TuplePerPage,arrayS.length))
      join(sc.parallelize(Block.BlockContentR), sc.parallelize(Block.BlockContentS))
      times2 = 1

    }
    Block.ReadR(arrayR.slice((times1-1)*TuplePerPage*(BlockSize-2),arrayR.length))
    while (times2*TuplePerPage <= arrayS.length){
      Block.ReadS(arrayS.slice((times2-1)*TuplePerPage,times2*TuplePerPage))
      times2+=1
      join(sc.parallelize(Block.BlockContentR), sc.parallelize(Block.BlockContentS))
    }
    Block.ReadS(arrayS.slice((times2-1)*TuplePerPage,arrayS.length))
    join(sc.parallelize(Block.BlockContentR), sc.parallelize(Block.BlockContentS))
    times2 = 1


    sc.stop()
  }
  def join[K: ClassTag, V:ClassTag, W:ClassTag](rdd1: RDD[(K,V)], rdd2: RDD[(K,W)],partitioner: Partitioner): RDD[(K,(V,W))]={
    rdd1.cogroup(rdd2,partitioner).flatMapValues(pair => for (v<-pair._1.iterator; w<-pair._2.iterator)yield(v,w))
  }
}
