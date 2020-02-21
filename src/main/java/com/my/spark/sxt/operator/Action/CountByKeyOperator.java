package com.my.spark.sxt.operator.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// 有shuffle：在每个Partition中各自以Map格式统计了数量
public class CountByKeyOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CountByKeyOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("xuruyun", 150),
                new Tuple2<String, Integer>("liangjinru", 160),
                new Tuple2<String, Integer>("liudehua", 170),
                new Tuple2<String, Integer>("zhangxueyou", 180),
                new Tuple2<String, Integer>("dakoumaya", 190),
                new Tuple2<String, Integer>("ouyang", 200),
                new Tuple2<String, Integer>("wangfei", 190),
                new Tuple2<String, Integer>("wangfei", 80)
        );

        JavaPairRDD<String, Integer> staffRDD = sc.parallelizePairs(scoreList);
        Map<String, Object> counts = staffRDD.countByKey();
        for(Map.Entry<String, Object> c : counts.entrySet()){
            System.out.println(c.getKey() + ":" + c.getValue());
        }

        sc.close();
    }

}
