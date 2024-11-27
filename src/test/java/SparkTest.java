import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SparkTest {

    @Test
    public void test1() {
        System.out.println("Hello to the Spark!");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkApp")
                .getOrCreate();

        try (JavaSparkContext sc = new JavaSparkContext(spark.sparkContext())) {

            List<Integer> integerList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

            JavaRDD<Integer> integerJavaRDD = sc.parallelize(integerList, 4);
            integerJavaRDD.foreach(i -> System.out.println(i));
            System.out.println("=====");

            JavaRDD<Double> doubleJavaRDD = integerJavaRDD.map(n -> Double.valueOf(n) / 10);
            doubleJavaRDD.foreach(i -> System.out.println(i));
            System.out.println("=====");

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

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}
