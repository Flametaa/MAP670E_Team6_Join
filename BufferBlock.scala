import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class BufferBlock(TuplePerPageIn: Int = 1000, BlockSizeIn: Int = 10) {
  // simulate the block
  var TuplePerPage: Int = TuplePerPageIn
  var BlockSize: Int = BlockSizeIn
  var times: Int = 0
  var BlockContentR: RDD[(Any,Any)] = null
  var BlockContentS: RDD[(Any,Any)] = null
  var BlockContentO: RDD[(Any,Any)] = null
  def ReadR[K: ClassTag, V:ClassTag](rdd: RDD[(K,V)]): Unit ={
    BlockContentR = rdd
  }
  def ReadS[K: ClassTag, W:ClassTag](rdd: RDD[(K,W)]): Unit ={
    BlockContentS = rdd
  }
  def join[K: ClassTag, V:ClassTag, W:ClassTag](rdd1: RDD[(K,V)], rdd2: RDD[(K,W)],partitioner: Partitioner): RDD[(K,(V,W))] ={
    BlockContentO = rdd1.cogroup(rdd2,partitioner).flatMapValues(pair => for (v<-pair._1.iterator; w<-pair._2.iterator)yield(v,w))
    return BlockContentO.take(TuplePerPage)
  }
  def clear(a:Int = 0, b:Int = 0,c:Int = 0): Unit ={
    if (a==1){
      BlockContentR = null
    }
    if (b==1){
      BlockContentS = null
    }
    if (c==1){
      BlockContentO = null
    }
  }
}
