package org.spark.Team6Join

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Main {
  def main(args : Array[String]) {

    val conf= new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "JoinSparkApp") 
    implicit val sc=new SparkContext(conf)
    
    val id1=0; val id2=1;  val N=4
    
    val RPath="inputs/authors.csv"; val SPath="inputs/posts.csv"
    
    val outputPath="Join_output"
    val gracejoins = new GraceHashJoinSpark(sc,RPath,SPath,outputPath, id1,id2,N)
    gracejoins.GraceJoin() //apply the gracejoin algorithm

    sc.stop()
    println("Session Closed. Thank You!")

        
    }
}
