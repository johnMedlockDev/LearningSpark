package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        doesn't load whole file into memory // it partitions parts of the file into memory in the nodes.
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

//        Applies a regex to every element in the document. removes all non words and lowercases them.
        JavaRDD<String> lettersOnlyRdd = initialRdd.map(v1 -> v1.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

//        attempt to remove blank spaces
        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(v1 -> v1.trim().length() > 0);

//        splits every element on the space adn convert it to a iterator for flatmap.
        JavaRDD<String> justWords = removedBlankLines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

//        removes double spaces and tabs that the first one missed
        JavaRDD<String> blankWordsRemoved = justWords.filter(v1 -> v1.trim().length() > 0);

//        filters the words off a list
        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(v1 -> Util.isNotBoring(v1));

//        conforms or maps all values to tuples forms of (key, 1)
        JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(s -> new Tuple2<>(s, 1L));

//        returns distinct keys with a summation of the values 1 + 1 + 1
        JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((v1, v2) -> v1 + v2);

//        reverses the order of the tuple because there is not sort by value method
        JavaPairRDD<Long, String> switched = totals.mapToPair(v1 -> new Tuple2<Long, String>(v1._2, v1._1));

//        acceding by default false makes it descending
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

//        take return a string because Spark realizes that you are no longer handling Big Data
        List<Tuple2<Long, String>> results = sorted.take(50);

        results.forEach(s -> System.out.println(s));


//        closes spark context
        sc.close();
    }

}

//parsing a file
//public class Main {
//
//    public static void main(String[] args) {
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
////        doesn't load whole file into memory // it partitions parts of the file into memory in the nodes.
//        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
//
////        Applies a regex to every element in the document. removes all non words and lowercases them.
//        JavaRDD<String> lettersOnlyRdd = initialRdd.map(v1 -> v1.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
//
////        attempt to remove blank spaces
//        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(v1 -> v1.trim().length() > 0);
//
////        splits every element on the space adn convert it to a iterator for flatmap.
//        JavaRDD<String> justWords = removedBlankLines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//
////        removes double spaces and tabs that the first one missed
//        JavaRDD<String> blankWordsRemoved = justWords.filter(v1 -> v1.trim().length() > 0);
//
////        filters the words off a list
//        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(v1 -> Util.isNotBoring(v1));
//
////        conforms or maps all values to tuples forms of (key, 1)
//        JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(s -> new Tuple2<>(s, 1L));
//
////        returns distinct keys with a summation of the values 1 + 1 + 1
//        JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((v1, v2) -> v1 + v2);
//
////        reverses the order of the tuple because there is not sort by value method
//        JavaPairRDD<Long, String> switched = totals.mapToPair(v1 -> new Tuple2<Long, String>(v1._2, v1._1));
//
////        acceding by default false makes it descending
//        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
//
////        take return a string because Spark realizes that you are no longer handling Big Data
//        List<Tuple2<Long, String>> results = sorted.take(50);
//
//        results.forEach(s -> System.out.println(s));
//
//
////        closes spark context
//        sc.close();
//    }
//
//}

//// text files
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
////        doesn't load whole file into memory // it partitions parts of the file into memory in the nodes.
//        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
//
////        file usage
//        initialRdd
//                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .foreach(s -> System.out.println(s));
//
////        closes spark context
//        sc.close();
//    }
//
//}

//filters and flatmaps
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
////        long form
//        JavaRDD<String> sentences = sc.parallelize(inputData);
//        JavaRDD<String> words = sentences.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//
////        cpu bug
////        words.collect().forEach(System.out::println);
////        words.foreach(s -> System.out.println(s));
//
//        JavaRDD<String> filteredWords = words.filter(v1 -> v1.length() > 1);
//        filteredWords.foreach(s -> System.out.println(s));
//
////        the one liner form
//        sentences.
////                flat map requires a iterator.
////                s.split won't work individual string.
////                Arrays.asList(s) won't work because it returns a list
//                flatMap(s -> Arrays.asList(s.split(" ")).iterator())
////                filter applies test the value for truthy or falsy if it's truthy it's return
//                .filter(v1 -> v1.length() > 1)
////                foreach applies a function to each element of the RDD
//                .foreach(s -> System.out.println(s));
//
////        closes spark context
//        sc.close();
//    }
//
//}

// using tuples
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

// basic syntax
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