package com.ibeifeng.sparkproject.spark.product;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AreaTop3FunctionMyTest {

    public static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate){
        String sql =
                "SELECT "
                        + "city_id,"
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
                        + "AND date>='" + startDate + "' "
                        + "AND date<='" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityid = row.getLong(0);
                        return new Tuple2<Long, Row>(cityid, row);
                    }
                });

        return cityid2clickActionRDD;
    }

    public static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        // 构建MySQL连接配置信息（直接从配置文件中获取）
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);

        // 通过SQLContext去从MySQL中查询数据
        DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();

        // 返回RDD
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        long cityid = Long.valueOf(String.valueOf(row.get(0)));
                        return new Tuple2<Long, Row>(cityid, row);
                    }
                });

        return cityid2cityInfoRDD;
    }

    public static void generateTempClickProductBasicTable(
            SQLContext sqlContext,
            JavaPairRDD<Long, Row> cityid2clickActionRDD,
            JavaPairRDD<Long, Row> cityid2cityInfoRDD) {

        // 执行join操作，进行点击行为数据和城市数据的关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD);
        JavaRDD<Row> dataFrameRDD = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                long cityId = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._1;
                long productid = clickAction.getLong(1);
                String cityName = cityInfo.getString(1);
                String areaName = cityInfo.getString(2);

                return RowFactory.create(cityId, cityName, areaName, productid);
            }
        });

        List<StructField> listField = new ArrayList<StructField>();
        listField.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        listField.add(DataTypes.createStructField("city_name", DataTypes.LongType, true));
        listField.add(DataTypes.createStructField("area", DataTypes.LongType, true));
        listField.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType structType = DataTypes.createStructType(listField);
        DataFrame df = sqlContext.createDataFrame(dataFrameRDD, structType);
        System.out.println("tmp_click_product_basic: " + df.count());

        df.registerTempTable("tmp_click_product_basic");
    }

}
