package org.lkathary.cft.service;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface PopularWordsService extends Serializable {

    List<String> TopWords(JavaRDD<String> lines, Integer num);
}
