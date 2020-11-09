import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class BNLJ(rdd1: RDD[(String, Array[String])], rdd2: RDD[(String, Array[String])]) {
  var rddL: RDD[(String, Array[String])] = null
  var rddR: RDD[(String, Array[String])] = null

  //Define the method for partition of the RDD
  private def hashPartition(numPartitions: Int) = {
    val partitioner = new HashPartitioner(numPartitions)
    this.rddL = rdd1.partitionBy(partitioner).persist()
    this.rddR = rdd2.partitionBy(partitioner).persist()
  }

  def Joins(): RDD[Array[String]] = {
    val result = rddL.zipPartitions(rddR)(
      (iterL, iterR) => {
        val indexKeyR = iterR.toList
        val sizeR = indexKeyR.size
        var joined: ArrayBuffer[Array[String]] = ArrayBuffer()
        for ((k, v) <- iterL) {
          for(num <- Range(0,sizeR)) {
            if(k==indexKeyR.apply(num)._1) {
              joined+=(v++indexKeyR.apply(num)._2)
            }
          }
        }
        joined.toIterator
      })
    return result
  }

  def join(): RDD[Array[String]] = {
    hashPartition(6)
    val joined = Joins()
    return joined
  }

}
