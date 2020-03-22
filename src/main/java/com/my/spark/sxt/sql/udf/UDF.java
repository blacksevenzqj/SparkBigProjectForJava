package com.my.spark.sxt.sql.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class UDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UDFJava").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        List<String> stringList = new ArrayList<String>();
        stringList.add("Leo");
        stringList.add("Marry");
        stringList.add("Jack");
        stringList.add("Tom");
        JavaRDD<String> rdd = sparkContext.parallelize(stringList);
        JavaRDD<Row> nameRDD = rdd.map(new Function<String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(String v1) throws Exception {
                return RowFactory.create(v1);
            }
        });

        List<StructField> fieldList = new ArrayList<StructField>();
        fieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(fieldList);
        DataFrame dataFrame = sqlContext.createDataFrame(nameRDD, structType);

        dataFrame.registerTempTable("name");
        sqlContext.udf().register("strLen", new UDF1<String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }

        }, DataTypes.IntegerType);

        sqlContext.sql("select name, strLen(name) from name").javaRDD().foreach(
                new VoidFunction<Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row);
                    }
                });


    }
}
