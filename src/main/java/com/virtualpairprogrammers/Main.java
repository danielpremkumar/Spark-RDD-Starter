package com.virtualpairprogrammers;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class Main {

	public static void main(String[] args) {
		List<Integer> inputData = new ArrayList<>();
		inputData.add(1);
		inputData.add(19);
		inputData.add(23);
		inputData.add(142);
		inputData.add(36);
		inputData.add(111);
	Integer[] a = new Integer[]{4, 12, 9, 5, 6};
		Integer[] b = new Integer[]{4, 10, 6, 12,9};
		List<Integer> c = new ArrayList(Arrays.asList(a));
		c.removeAll(Arrays.asList(b));
		System.out.println(c.toString());


		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> myRDD = sc.parallelize(inputData);

		Integer result = myRDD.reduce((value1, value2) -> value1 + value2);


		System.out.println(result);
		JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));

		sqrtRDD.collect().forEach(System.out::println);

		System.out.println(sqrtRDD.count());
		Long count = sqrtRDD
				.map(value -> 1L)
				.reduce((val1, val2) -> val1 + val2);
		System.out.println(count);
		sc.close();

		String word = "dANIEL";
		System.out.println(reverse(word));
		JavaRDD<Tuple2<Integer, Double>> intPlusSqrtRDD = myRDD.map(value -> new Tuple2<>(value,Math.sqrt(value)));

		List<String> inputLog = new ArrayList<>();
		inputLog.add("WARN: Tuesday 4 September 0405");
		inputLog.add("ERROR: Tuesday 4 September 0408");
		inputLog.add("FATAL: Wednesday 5 September 1632");
		inputLog.add("ERROR: Friday 7 September 1854");
		inputLog.add("WARN: Saturday 8 September 1942");

		//JavaPairRDD example
		JavaSparkContext sc1 = new JavaSparkContext(conf);
		sc1.parallelize(inputLog)
				.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
				.reduceByKey( (val1, val2 ) -> val1 + val2)
				.foreach(tuple ->  System.out.println(tuple._1 + " has " + tuple._2 + " instances."));

		// groupbykey version

		sc1.parallelize(inputLog)
				.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
				.groupByKey()
				.foreach(tuple ->  System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances."));

		// flatMap example
		sc1.parallelize(inputLog)
				.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
				.filter(word1 -> word1.length() > 1)
				.foreach(val -> System.out.println(val));

		sc1.close();
	}

	private static String reverse(String word) {
		StringBuilder reverseword = new StringBuilder();
		char[] chars = word.toCharArray();
		Stream.of(chars).peek(System.out::print);
		for(int i=chars.length-1;i>=0; i--) {
			reverseword.append(chars[i]);
		}
		return reverseword.toString();
	}
}
