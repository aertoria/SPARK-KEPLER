package com.salesforce.kepler;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Spark if code in Java, make sure spark 1.2.0, scala 2.10.4, spark streaming 1.2.0
 * @author ethan.wang
 *
 */
public class WordCount{
	public static void main(String[] args){
		System.out.println("KEPLER START");
		
		SparkConf conf = new SparkConf()
				.setAppName("wordcount")
				.setMaster("local");
//				.setSparkHome("/Users/ethan.wang/Desktop/Spark/SPARKSTUDY/spark-2.1.0-bin-hadoop2.7");
		/**
		 * BASIC
		 */
//		JavaSparkContext jsc = new JavaSparkContext(conf);
//		List<Integer> mylist= new ArrayList(Arrays.asList(1,2,3,4));
//		JavaRDD<Integer> rdds=jsc.parallelize(mylist,2)
//								.map(i -> i+5)
//								.filter(i -> i>6);
//		
//		System.out.println("printing result....");
//		System.out.println(rdds.collect());
//		System.out.println(rdds.toDebugString());
//	
//		System.out.println(rdds.reduce((a,b) -> a+b));
//		System.out.println("\n\n\n\n\n");

		
		/**
		 * Streaming
		 */
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		System.out.println("\n\ncount");
		System.out.println(lines);
		
//		lines.transform(rdd -> {
//			System.out.println("current rdd");
//			System.out.println(rdd.toDebugString());
//			return rdd;
//		});
		
		JavaDStream<String> words=lines.flatMap(
				new FlatMapFunction<String,String>(){
					@Override
					public Iterable<String> call(String t){
						return Arrays.asList(t.split(""));
					}
		});
		
//		JavaDStream<String> words2=lines.flatMap(t -> Arrays.asList(t.split(" ")));
		
		
		
		JavaDStream<Integer> pairs = words.map(s -> 1);
		System.out.println("\n\n\ntill");
		pairs.print();
		
//		JavaDStream<Integer> wordscounts = pairs.reduce((c1,c2)->c1+c2);
//		wordscounts.print();
		
//		JavaPairDStream<String, Integer> pairs = words.map(
//				new PairFunction<String, String, Integer>(){
//					@Override
//					public Tuple2<String, Integer> call(String t){
//						return new Tuple2<String, Integer>(t,1);
//					}
//		});
		
		
//		lines.foreachRDD(r -> null);
		
//		lines.transform(new Function2<JavaRDD<String>,time,JavaRDD<String>>(){
//			@Override
//			public JavaRDD<String> call(JavaRDD<String> v1, time v2) throws Exception {
//				// TODO Auto-generated method stub
//				return null;
//			}
//		});
		
		
//		lines.foreachRDD(rdd -> System.out.println(rdd.toDebugString()));
		jssc.start();
		jssc.awaitTermination();
		
		
//		
		
		
	}
}
