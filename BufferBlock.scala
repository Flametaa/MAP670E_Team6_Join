class BufferBlock(TuplePerPageIn: Int = 1000, BlockSizeIn: Int = 10) {
  // simulate the block
  var TuplePerPage: Int = TuplePerPageIn
  var BlockSize: Int = BlockSizeIn
  var times: Int = 0
  var BlockContentR: Array[String] = new Array[String](TuplePerPageIn*(BlockSizeIn-2))
  var BlockContentS: Array[String] = new Array[String](TuplePerPageIn)
  var BlockContentO: Array[String] = new Array[String](TuplePerPageIn)
  def ReadR(data: Array[String]): Unit ={
    BlockContentR = data
  }
  def ReadS(data: Array[String]): Unit ={
    BlockContentS = data
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
