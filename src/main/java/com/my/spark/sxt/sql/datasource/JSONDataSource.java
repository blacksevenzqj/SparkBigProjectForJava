package com.my.spark.sxt.sql.datasource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


// https://www.cnblogs.com/weiyiming007/p/11286837.html
public class JSONDataSource {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JSONDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String path = "E:\\code\\workSpace\\mylearnObject\\git_directory\\Spark\\Spark_Big_Project\\spark-project\\src\\main\\java\\com\\my\\spark\\sxt\\data\\sql\\";
        DataFrame dataFrame = sqlContext.read().format("json").load(path + "student.json");

        dataFrame.registerTempTable("student_scores");
        DataFrame goodStudentScoresDF  = sqlContext.sql("select * from student_scores where score >= 80");
        List<String> goodStudentNames = goodStudentScoresDF.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("name");
            }
        }).collect();
        for(String str : goodStudentNames){
            System.out.println(str);
        }

        // 然后针对JavaRDD<String>，创建DataFrame
        // （针对包含json串的JavaRDD，创建DataFrame）
        List<String> studentInfoJSONs = new ArrayList<String>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");
        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs);
        DataFrame studentInfosDF = sqlContext.read().json(studentInfoJSONsRDD);

        // 针对学生基本信息DataFrame，注册临时表，然后查询分数大于80分的学生的基本信息
        studentInfosDF.registerTempTable("student_infos");

        String sql = "select name,age from student_infos where name in (";
        for(int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";
            if(i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";

        DataFrame goodStudentInfosDF = sqlContext.sql(sql);

        // 然后将两份数据的DataFrame，转换为JavaPairRDD，执行join transformation
        // （将DataFrame转换为JavaRDD，再map为JavaPairRDD，然后进行join）
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD =
                goodStudentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0),
                                Integer.valueOf(String.valueOf(row.getLong(1))));
                    }

                }).join(goodStudentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0),
                                Integer.valueOf(String.valueOf(row.getLong(1))));
                    }
                }));


        // 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
        // （将JavaRDD，转换为DataFrame）
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(
                new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Row call(
                            Tuple2<String, Tuple2<Integer, Integer>> tuple)
                            throws Exception {
                        return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
                    }
                });

        // 创建一份元数据，将JavaRDD<Row>转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);

        // 将好学生的全部信息保存到一个json文件中去
        // （将DataFrame中的数据保存到外部的json文件中去）
//        goodStudentsDF.write().format("json").mode(SaveMode.Overwrite).save("hdfs://spark1:9000/spark-study/good-students");

        sc.close();
    }

}
