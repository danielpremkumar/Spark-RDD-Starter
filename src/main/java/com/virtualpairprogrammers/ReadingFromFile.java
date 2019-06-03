package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReadingFromFile {

    public static void main(String[] args) {
        // Only for Windows
        System.setProperty("hadoop.home.dir", "C:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word1 -> word1.length() > 1)
                .foreach(val -> System.out.println(val));


        sc.close();
    }
}
