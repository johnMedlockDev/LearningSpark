package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv("src/main/resources/biglog.txt");

//        makes functions less messy
//        import static org.apache.spark.sql.functions.*;
        dataset = dataset
//                selects the column with the header of level
                .select(col("level"),
//                takes the datetime and converts to full month name as month
                        date_format(col("datetime"), "MMMM").alias("month"),
//                takes the datetime and converts it to the format of it's number of month as monthnum and cast it to real int value
                        date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

//        array of objects
        Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

//        converts array of objects to  list of object
        List<Object> columns = Arrays.asList(months);

//        start on the row column
//        when you group you need a agg function
//           dataset = dataset.groupBy("level").pivot("month").count();

//        same as before but it orders the months off how the list is instead of alphabetically
        dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);

        dataset.show(100);


        sparkSession.close();
    }
//    public static void main(String[] args) {
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
//                .getOrCreate();
//
//        Dataset<Row> dataset = sparkSession.read()
//                .option("header", true)
//                .csv("src/main/resources/biglog.txt");
//
////        makes functions less messy
////        import static org.apache.spark.sql.functions.*;
//        dataset = dataset
////                selects the column with the header of level
//                .select(col("level"),
////                takes the datetime and converts to full month name as month
//                date_format(col("datetime"),"MMMM").alias("month"),
////                takes the datetime and converts it to the format of it's number of month as monthnum and cast it to real int value
//                date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
//
//        dataset = dataset
////                groups by the column with header level
//                .groupBy(col("level"),
////                        and month
//                        col("month"),
////                        and monthnum and counts the values that occur
//                        col("monthnum")).count();
//
//        dataset = dataset
////                sorts the data by column monthnum
//                .orderBy(col("monthnum"),
////                and column level
//                col("level"));
//
//        dataset = dataset
////                removes the monthnum column from the view
//                .drop(col("monthnum"));
//
//        dataset.show(100);
//
//
//        sparkSession.close();
//    }
//
//    public static void main(String[] args) {
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
//                .getOrCreate();
//
//        Dataset<Row> dataset = sparkSession.read()
//                .option("header", true)
//                .csv("src/main/resources/biglog.txt");
//
//        dataset = dataset.selectExpr("level", "date_format(datetime,'MMMM') as month");
//
//        dataset.show(100);
//
//
//        sparkSession.close();
//    }
//    public static void main(String[] args) {
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
//        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
//
//        dataset.createOrReplaceTempView("logging_table");
//
//        //        produces a month full month name with alias count to get the actual values group by like before
////        Dataset<Row> results = sparkSession
////                .sql("select level, date_format(datetime,'MMMM') as month,  cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total from logging_table group by level, month order by monthnum");
//
//        //        optimized version you can make functions run i order by without declaring them first
//        Dataset<Row> results = sparkSession
//                .sql("select level, date_format(datetime,'MMMM') as month,  count(1) as total from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");
////
//////        drops the mothnum column
////        results =  results.drop("monthnum");
//
////        shows the top 100 rows of the dataset
//        results.show(100);
//
////        closes the session
//        sparkSession.close();
//    }

}
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
//        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
//
//        dataset.createOrReplaceTempView("logging_table");
//
//        //        produces a month full month name with alias count to get the actual values group by like before
//        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logging_table group by level, month");
//
////        shows the top 100 rows of the dataset
//        results.show(100);
//
//        results.createOrReplaceTempView("results_table");
//
////        prints the total values within the table
//        Dataset<Row> totals = sparkSession.sql("select sum(total) from results_table");
//        totals.show();
//
////        closes the session
//        sparkSession.close();
//    }
//
//}
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
////        List for inMemory dataset
//        List<Row> inMemory = new ArrayList<>();
//
//        //        values for dataset
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//        inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
//
////        Structfields are headers
//        StructField[] fields = new StructField[]{
//                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
//        };
//
////        Schema of the database
//        StructType schema = new StructType(fields);
//
////        creation of the dataset obj
//        Dataset<Row> dataset = sparkSession.createDataFrame(inMemory, schema);
//
//        dataset.createOrReplaceTempView("logging_table");
//
//
//////        produces a 4 digit year
////        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'y') from logging_table");
////
////        //        produces a 2 digit year
////        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'yy') from logging_table");
////
////        //        produces a month number
////        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'M') from logging_table");
////
////        //        produces a month number with zero if applicable
////        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'MM') from logging_table");
////
////        //        produces a month 3 abri month
////        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'MMM') from logging_table");
//
//        //        produces a month full month name with alias
//        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime,'MMMM') as month from logging_table");
//
////        shows the top 20 rows of the dataset
//        results.show();
//
////        closes the session
//        sparkSession.close();
//    }
//
//}

//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
////        List for inMemory dataset
//        List<Row> inMemory = new ArrayList<>();
//
//        //        values for dataset
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//        inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));
//
////        Structfields are headers
//        StructField[] fields = new StructField[]{
//                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
//        };
//
////        Schema of the database
//        StructType schema = new StructType(fields);
//
////        creation of the dataset obj
//        Dataset<Row> dataset = sparkSession.createDataFrame(inMemory, schema);
//
//        dataset.createOrReplaceTempView("logging_table");
//
////        causes a crash because you need an aggregation function as it will return multiple values
////        Dataset<Row> results = sparkSession.sql("select level, datetime from logging_table group by level");
////        you have to do something like this
////        Dataset<Row> results = sparkSession.sql("select level, count(datetime) from logging_table group by level order by level");
////        collect_list pushes all the values into an array
//        Dataset<Row> results = sparkSession.sql("select level, collect_list(datetime) from logging_table group by level order by level");
//
////        shows the top 20 rows of the dataset
//        results.show();
//
////        closes the session
//        sparkSession.close();
//    }
//
//}
//    public static void main(String[] args) {
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
//        Dataset<Row> dataset = sparkSession
//                .read()
//                .option("header", true)
//                .csv("src/main/resources/exams/students.csv");
//
////        creates a view and assigns it a name. You can write sql like statements on them
//        dataset.createOrReplaceTempView("my_students_table");
//
////        sql like statement
////        Dataset<Row> results = sparkSession.sql("select score, year from my_students_table where subject='French'");
////        Dataset<Row> results = sparkSession.sql("select max(score) from my_students_table where subject='French'");
//        Dataset<Row> results = sparkSession.sql("select distinct(year) from my_students_table order by year desc ");
//
//
//        results.show();
//
//        sparkSession.close();
//    }
//
//}

// using filters
//public class Main {
//
//    public static void main(String[] args) {
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
//        Dataset<Row> dataset = sparkSession
//                .read()
//                .option("header", true)
//                .csv("src/main/resources/exams/students.csv");
//
////        filters are like sql where clauses
////        Dataset<Row> modernArtResults =  dataset.filter("subject = 'Modern Art' AND year >= 2007");
//
////        filter with columns does the same as other filter
//        Column subjectColumn = dataset.col("subject");
//        Column yearColumn = dataset.col("year");
//
//        Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
//        .and(yearColumn.geq(2007)));
//
//        modernArtResults.show();
//
//        sparkSession.close();
//    }
//
//}

// basics of SparkSQL
//    public static void main(String[] args) {
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
////        sparkSQL boilerplate
//        SparkSession sparkSession = SparkSession.builder()
//                .appName("SparkSQL").master("local[*]")
//                .config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();
//
////        loading a file into memory
//        Dataset<Row> dataset = sparkSession
//                .read()
//                .option("header", true)
//                .csv("src/main/resources/exams/students.csv");
//
////        prints first 20 rows
//        dataset.show();
//
////        count is the total number of rows
//        System.out.println("There are " + dataset.count() + " records");
//
////        returns the first row
//        Row firstRow = dataset.first();
//
////        return 2 index of the row || returns a object so to print it use toString()
////        String secondIndex = firstRow.get(2).toString();
////        if you have headers you can use getAs
//        String secondIndex = firstRow.getAs("subject").toString();
//
//
//        System.out.println(secondIndex);
////        closes session
//        sparkSession.close();
//    }
//
//}

// RDD stuff
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