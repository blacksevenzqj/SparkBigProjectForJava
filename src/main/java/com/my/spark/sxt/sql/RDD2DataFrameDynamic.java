package com.my.spark.sxt.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameDynamic {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\";
        JavaRDD<String> text = sc.textFile(path + "student.txt");

        JavaRDD<Row> rows = text.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] str = v1.split(",");
                return RowFactory.create(Integer.valueOf(str[0]),str[1],Integer.valueOf(str[2]));
            }
        });

        // 创建Schema
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        DataFrame dataFrame = sqlContext.createDataFrame(rows, schema);
        dataFrame.registerTempTable("student");

        DataFrame tempDF = sqlContext.sql("select * from student where age <= 18");
        JavaRDD<Row> sdRDD = tempDF.toJavaRDD();
        sdRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.getAs("id") + " " + row.getAs("name") + " " + row.getAs("age"));
            }
        });

        List<Row> list = sdRDD.collect();
        for(Row row : list){
            System.out.println(row.getAs("id") + " " + row.getAs("name") + " " + row.getAs("age"));
        }

        sc.close();
    }

}
