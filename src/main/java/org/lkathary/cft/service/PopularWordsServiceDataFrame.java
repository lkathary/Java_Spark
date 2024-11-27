package org.lkathary.cft.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

public class PopularWordsServiceDataFrame implements PopularWordsService {

    private final JavaSparkContext sc;

    public PopularWordsServiceDataFrame(JavaSparkContext sc) {
        this.sc = sc;
    }

    @Override
    public List<String> TopWords(JavaRDD<String> lines, Integer num) {

        JavaRDD<Row> rdd = lines
                .flatMap(x -> Arrays.asList(x.split("[,.!;\\s]+")).iterator())
                .map(RowFactory::create);

        SQLContext sqlContext = SQLContext.getOrCreate(sc.sc());
        Dataset<Row> ds = sqlContext.createDataFrame(rdd, DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("words", DataTypes.StringType, true)
        }));
//        ds.show();

        Dataset<Row> dsSorted = ds.withColumn("words", lower(col("words")))
                .groupBy("words").count()
                .orderBy(col("count").desc());

//        dsSorted.show();
//        Row[] rows = (Row[]) dsSorted.take(num);

        return Arrays.stream((Row[])dsSorted.take(num))
                .map(x -> x.getString(0))
                .toList();

    }
}
