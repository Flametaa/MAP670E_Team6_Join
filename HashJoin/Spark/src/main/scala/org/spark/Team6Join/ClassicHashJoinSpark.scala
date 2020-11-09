package org.spark.Team6Join

import org.apache.spark.SparkContext

import java.io._
import java.util._

import scala.collection.mutable.HashMap 
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable._

class ClassicHashJoinSpark
                  (  private val sc: SparkContext,
                    private var pathDB1: String,
                    private var pathDB2: String,
                    private var id1: Int,
                    private var id2 : Int)
 {
  
        def buildTable(path : String, id: Int)={
          val map: HashMap[String,ArrayBuffer[String]] = new HashMap [String, ArrayBuffer[String]]()
          try {
          var dbfile=sc.textFile(path)
          var splitRdd=dbfile.map(line => line.split("/n")).collect()
              splitRdd.foreach(x=>  {          
                    var line=x(0)
                    var lst=ArrayBuffer(line.split(','): _*)
                    val key = line.split(',')(id)
                    var v=map.get(key) getOrElse ArrayBuffer[String]()    
                    v+=line
                    map.put(key,v)
                                                           })
              }
           catch {    case x: IOException   => 
                    { 
                      // Displays this if input/output  
                      // exception is found 
                      println("Input/output Exception in Classic Hash Join.scala/buildTable") 
                    } 
                  }
           map
        }
           
       def join()= {
        var map = buildTable(pathDB1,id1)
        var join_result=""
        try
        {
                var dbfile=sc.textFile(pathDB2)
                var splitRdd=dbfile.map(line => line.split("/n")).collect()
                splitRdd.foreach(x=>  {
                  val line=x(0)
                  val key = line.split(",")(id2)
                  val ss=ArrayBuffer(line.split(','): _*)
                  ss.remove(id2) //Remove the key as we already have it in the first database
                  var lst=map.get(key) 
                  if (lst!=null)
                  {
                    lst.toStream.foreach( r=> {
                      
                      join_result+=r.mkString(",")+ss.mkString(",")+"\n"
                    })
                  }
                })

        }
        
        catch {
          case x : IOException =>
            {
              println("Input Output Exception In Classic Hash Join.java")
            }
        }
        join_result
      }   

}