package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopWordCount {

    public static void main(String[] args) {
        // Only for Windows
        System.setProperty("hadoop.home.dir", "C:/hadoop");

        // Below Program gives Top 10 highest word count occurrences from the big data (boring words filtered)
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
            List<Tuple2<Long, String>> sorted = initialRDD.map(s -> s.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                    .filter(s-> s.trim().length() > 0)
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                    .filter(s -> s.trim().length() > 0 && Util.isNotBoring(s))
                    .mapToPair(word -> new Tuple2<>(word, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
                    .sortByKey(false)
                    .take(10);
            sorted.forEach(System.out::println);
        }
    }
}
