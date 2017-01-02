package com.salesforce.kepler2
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._


object SparkWordCount {
    def main(args: Array[String]){
    
    print("Spark kepler started\n")
    
    val sc = new SparkContext(
        "local",
        "WorldCount",
        "/Users/ethan.wang/Desktop/Spark/SPARKSTUDY/spark-2.1.0-bin-hadoop2.7",
        Nil,
        Map())
    
    val rdd = sc.parallelize(List(1,2,3,4), 2).map(i => i+10)
    print(rdd.count())
    print("\n\n\n\n")
    print(rdd.reduce(_+_))
    print("\n\n\n\n")
    print(rdd.toDebugString)
    print("\n\n\n\n")
  }
}