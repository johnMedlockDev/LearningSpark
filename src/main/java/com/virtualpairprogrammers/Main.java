package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        long form
        JavaRDD<String> sentences = sc.parallelize(inputData);
        JavaRDD<String> words = sentences.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

//        cpu bug
//        words.collect().forEach(System.out::println);
//        words.foreach(s -> System.out.println(s));

        JavaRDD<String> filteredWords = words.filter(v1 -> v1.length() > 1);
        filteredWords.foreach(s -> System.out.println(s));

//        the one liner form
        sentences.
//                flat map requires a iterator.
//                s.split won't work individual string.
//                Arrays.asList(s) won't work because it returns a list
                flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                filter applies test the value for truthy or falsy if it's truthy it's return
                .filter(v1 -> v1.length() > 1)
//                foreach applies a function to each element of the RDD
                .foreach(s -> System.out.println(s));


//        closes spark context
        sc.close();
    }

}

//public class Main {
//
//    public static void main(String[] args) {
//        List<String> inputData = new ArrayList<>();
//        inputData.add("WARN: Tuesday 4 September 0405");
//        inputData.add("ERROR: Tuesday 4 September 0408");
//        inputData.add("FATAL: Wednesday 5 September 1632");
//        inputData.add("ERROR: Friday 7 September 1854");
//        inputData.add("WARN: Saturday 8 September 1942");
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
////        all operations are fluent
//
////        loads all the data into memory
//        sc.parallelize(inputData)
////                maps or conforms all the data to a Tuple2 (k,v) split on the :
////                the first being the value of the split
////                the second being x = 1
////                So (WARN,1), (ERROR,1), (FATAL,1), etc
//                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
////                Says hey give me all the distinct keys and add all the values 1+1,etc
//                .reduceByKey((value1, value2) -> value1 + value2)
////                for each Tuple2 sout the first element of the tuple and the second element of the tuple.
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
//
//        sc.close();
//    }
//
//}


//
//public class Main {
//    public static void main(String[] args) {
//
//        List<Double> inputData = new ArrayList<>();
//        inputData.add(35.5);
//        inputData.add(32345.5);
//        inputData.add(224.53);
//
////        remove boilerplate logging
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
////        local context with max threads available
//        SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
////        Wrapper class
//        JavaRDD<Double> myRDD = sc.parallelize(inputData);
//
////        closes JavaSparkContext
//        sc.close();
//
//    }
//}