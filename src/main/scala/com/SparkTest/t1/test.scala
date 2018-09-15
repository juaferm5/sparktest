package com.SparkTest.t1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object test {

  def main(args: Array[String]) {
    // Initial code to spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the next Spark RDD
    val textFile = sc.textFile("/home/juan/IdeaProjects/untitled/src/main/resources/shakespeare.txt")
    //val sw = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
    val wordCounts = textFile.flatMap(line => line.split(" ")).map(x=> (x,1)).reduceByKey((a,b)=>(a+b))
    wordCounts.foreach(println)

    //System.out.println("Total:" + wordCounts.count())
    //count.saveAsTextFile("./out.txt")
    //count.saveAsTextFile("./out.txt")
  }
}
