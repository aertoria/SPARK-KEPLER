package com.salesforce.kepler2;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

public class JSC {
	public static void main(String[] args){
		System.out.println("s");
	
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");

		
	}
}
