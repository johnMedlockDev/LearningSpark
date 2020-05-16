package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(32345.5);
        inputData.add(224.53);

//        remove boilerplate logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);

//        local context with max threads available
        SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        Wrapper class
        JavaRDD<Double> myRDD = sc.parallelize(inputData);

//        closes JavaSparkContext
        sc.close();

    }
}
