package com.my.spark.sxt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class TestStoragelLevel {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TestStoragelLevel");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> text = sc.textFile("C:\\Users\\dell\\Desktop\\Hadoop.txt");
//        text.cache();
        text.persist(new StorageLevel(false, true, false, true, 1));

        long start = System.currentTimeMillis();
        long count = text.count();
        System.out.println(count);
        long end = System.currentTimeMillis();
        System.out.println((end - start));

        long start2 = System.currentTimeMillis();
        long count2 = text.count();
        System.out.println(count2);
        long end2 = System.currentTimeMillis();
        System.out.println((end2 - start2));


        sc.close();
    }

}
