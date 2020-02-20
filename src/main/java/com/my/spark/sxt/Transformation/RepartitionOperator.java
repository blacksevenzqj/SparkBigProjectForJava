package com.my.spark.sxt.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// 用于增加Partition
public class RepartitionOperator {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RepartitionOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList("xuruyun1", "xuruyun2", "xuruyun3", "xuruyun4", "xuruyun5",
                "xuruyun7", "xuruyun8", "xuruyun9", "xuruyun10", "xuruyun11", "xuruyun12");

        JavaRDD<String> staffRDD = sc.parallelize(names, 3);
        JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    String staff = iterator.next();
                    list.add("部门[" + (index + 1) + "]" + staff);
                }
                return list.iterator();
            }
        }, true);
        for(String staffInfo : staffRDD2.collect()){
            System.out.println(staffInfo);
        }

        JavaRDD<String> staffRDD3 = staffRDD2.repartition(6); // 最终是调用了coalesce
        JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()){
                    String staff = iterator.next();
                    list.add("部门[" + (index + 1) + "]" + staff);
                }
                return list.iterator();
            }
        }, true);
        for(String staffInfo : staffRDD4.collect()){
            System.out.println(staffInfo);
        }

    }
}
