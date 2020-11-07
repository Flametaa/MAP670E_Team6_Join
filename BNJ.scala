import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class BNJ[K: ClassTag, V:ClassTag, W:ClassTag](rdd: RDD[(K,V)], other_rdd: RDD[(K,W)], BlockSize_out: Int = 10, tuplePerPageIN: Int = 1000) {
  var buffer: BufferBlock = null
  val RDD0: RDD[(K,V)] = rdd
  val RDD1: RDD[(K,W)] = other_rdd
  var BlockSize: Int = BlockSize_out
  var TupleNumber: Int = tuplePerPageIN

  def Bl_join(): RDD[(K,(V,W))] ={
    //for 1->  block_size(m)
      //read the data in to Buffer_block
      //m-2 page for rdd
      //1 page for other_rdd
      //one page for output
      //using for loop to check if some tuple match the condition
    val numberR = this.RDD0.count()
    val numberS = this.RDD1.count()
    var RDD3 = RDD[(K,V)] = RDD0
    var RDD4 = RDD[(K,W)] = RDD1
    var RDD5 = RDD[(K,(V,W))] = null
    var times1: Int = 0
    var times2: Int = 0
    while (times1*tuplePerPageIN < numberR){
      buffer.ReadR(RDD0.take((this.BlockSize-2)*this.TupleNumber))
      times1+=1
      while (times2*tuplePerPageIN < numberS){
        buffer.ReadS(RDD1.take(this.TupleNumber))
        (buffer.join(buffer.BlockContentR,buffer.BlockContentS))
        buffer.clear(0,1,1)
        times2+=1
      }
      buffer.clear(0,1,1)
    }
  }


}
