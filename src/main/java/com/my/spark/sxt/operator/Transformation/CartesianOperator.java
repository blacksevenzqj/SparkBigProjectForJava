package com.my.spark.sxt.operator.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

// 笛卡尔积
public class CartesianOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CartesianOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);


        sc.close();
    }

}
