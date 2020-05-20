package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ExamResults {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession
                .read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/exams/students.csv");

        Column score = dataset.col("score");


//        dataset = dataset
//        .groupBy("subject")
//        .pivot("year")
//        .agg(avg(col("score")));
        dataset = dataset
//                groupby subject row column
                .groupBy("subject")
//                defines the header column
                .pivot("year")
//                agg because it have more than one value, round in this case reduces decimals to two on the column score
//                based on their averages as average
                .agg(round(avg(col("score")), 2).alias("average"),
//                agg because it have more than one value, round in this case reduces decimals to two on the column score
//                based on their stdev as stdev
                        round(stddev(col("score")), 2).alias("stdev"));

        dataset.show();
    }

}
//        use agg to cast if you need to
//        dataset = dataset
//                 groups by the subject
//                .groupBy("subject")
//                  aggregates the max score with explicit cast to datatype int as max score
//                .agg(max(col("score").cast(DataTypes.IntegerType)).alias("max score"),
//                  aggregates the max score with explicit cast to datatype int as min score
//                        min(col("score").cast(DataTypes.IntegerType)).alias("min score"));

//        same but the agg function didn't require cast really just the higher level of abstraction max m
//        dataset = dataset
//                .groupBy("subject")
//                .agg(max(col("score")).alias("max score"),
//                        min(col("score")).alias("min score"));