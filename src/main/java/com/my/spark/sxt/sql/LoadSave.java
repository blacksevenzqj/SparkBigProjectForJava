package com.my.spark.sxt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class LoadSave {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadSave");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().load("E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\student.parquet");
        df.printSchema();
        df.show();

        sc.close();
    }

}
