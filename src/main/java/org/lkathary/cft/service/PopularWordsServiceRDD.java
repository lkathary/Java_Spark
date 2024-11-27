package org.lkathary.cft.service;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;


import java.util.Arrays;
import java.util.List;

public class PopularWordsServiceRDD implements PopularWordsService {
    @Override
    public List<String> TopWords(JavaRDD<String> lines, Integer num) {

        return lines
                .map(String::toLowerCase)
                .flatMap(x -> Arrays.asList(x.split("[,.!\\s]+")).iterator())
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                        .reduceByKey(Integer::sum)
                        .mapToPair(Tuple2::swap)
                        .sortByKey(false)
                        .map(Tuple2::_2)
                        .take(num);
    }
}
