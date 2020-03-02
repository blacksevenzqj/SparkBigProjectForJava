package com.my.spark.sxt.operator.Action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ForeachPartitionOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ForeachOperator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = sc.parallelize(data,3);

        //获得分区ID
        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                LinkedList<String> linkedList = new LinkedList<String>();
                while(v2.hasNext()){
                    linkedList.add(v1 + "=" + v2.next());
                }
                return linkedList.iterator();
            }
        },false);
        System.out.println(partitionRDD.collect());
        javaRDD.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> integerIterator) throws Exception {
                System.out.println("___________begin_______________");
                while(integerIterator.hasNext())
                    System.out.print(integerIterator.next() + "      ");
                System.out.println("\n___________end_________________");
            }
        });

    }
}
