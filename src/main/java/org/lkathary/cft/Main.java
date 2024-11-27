package org.lkathary.cft;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.lkathary.cft.service.PopularWordsService;
import org.lkathary.cft.service.PopularWordsServiceDataFrame;
import org.lkathary.cft.service.PopularWordsServiceRDD;

import java.util.Arrays;
import java.util.List;


public class Main {
    public static void main(String[] args) {

        System.out.println("Hello to the Spark!");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkApp")
                .getOrCreate();

        try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {

            System.out.println("1. task ===========");

            List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            JavaRDD<Integer> integerJavaRDD = sc.parallelize(integerList, 4);
            integerJavaRDD.foreach(i -> System.out.println(i));
            System.out.println("=====");

            JavaRDD<Double> doubleJavaRDD = integerJavaRDD.map(n -> Double.valueOf(n) / 10);
            doubleJavaRDD.foreach(i -> System.out.println(i));

            List<Double> doubleList = doubleJavaRDD.collect();
            System.out.println("The list is in the same order as the original list!!!");
            System.out.println(doubleList);
            System.out.println("=====");

            // explore RDD partitioning properties -- glom() keeps the RDD as
            // an RDD but the elements are now lists of the original values --
            // the resulting RDD has an element for each partition of
            // the original RDD
            JavaRDD<List<Double>> partitionsRDD = doubleJavaRDD.glom();
            System.out.println("*** We _should_ have 4 partitions");
            System.out.println("*** (They can't be of equal size)");
            System.out.println("*** # partitions = " + partitionsRDD.count());
            // specifying the type of l is not required here but sometimes it's useful for clarity
            partitionsRDD.foreach((List<Double> l) -> {
                // A string for each partition so the output isn't garbled
                // -- remember the RDD is still distributed so this function
                // is called in parallel
                StringBuffer sb = new StringBuffer();
                for (Double d : l) {
                    sb.append(d);
                    sb.append(" ");
                }
                System.out.println(sb);
            });
            System.out.println("=====");


            // 2. Из заданного файла собрать сторокаи длинной до 16 символов
            // отсортировать их в лексикографическом порядке и поместить их в колексцию в UpperCase
            System.out.println("2. task ===========");

            JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/README.md");
//            JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/russia-blacklist.txt");
            System.out.println(stringJavaRDD.count());
            stringJavaRDD.foreach(x -> System.out.println(x));

            List<String> res = stringJavaRDD
                    .filter(x -> x.length() < 16 && x.length() > 0)
                    .sortBy(x -> x, true, 4)
                    .map(String::toUpperCase)
                    .collect();
            System.out.println(res);



        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

        // 3. Из заданного файла найти 30 наиболее часто встречаемых слов
        // отсортировать по частоте встречаемости и поместить их в колекцию в LowerCase
        System.out.println("3. task ===========");

        String name = "src/main/resources/fish.txt";

        SparkConf sparkConf = new SparkConf()
                .setAppName("Second")
                .setMaster("local[*]");

        try(JavaSparkContext sc = new JavaSparkContext(sparkConf)){

            // v1 RDD
            JavaRDD<String> stringJavaRDD = sc.textFile(name);
            PopularWordsService popularWordsServiceRDD = new PopularWordsServiceRDD();
            List<String> res1 = popularWordsServiceRDD.TopWords(stringJavaRDD, 20);

            System.out.println(res1);

            // v2 DataFrame
            PopularWordsService popularWordsServiceDataFrame = new PopularWordsServiceDataFrame(sc);
            List<String> res2 = popularWordsServiceDataFrame.TopWords(stringJavaRDD, 20);

            System.out.println("========");
            System.out.println(res2);

        }
    }
}