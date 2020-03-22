package com.ibeifeng.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class SessionMyTest {

    public static void main(String[] args) {
        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
//				.set("spark.default.parallelism", "100")
                .set("spark.storage.memoryFraction", "0.5")
                .set("spark.shuffle.consolidateFiles", "true")
                .set("spark.shuffle.file.buffer", "64")
                .set("spark.shuffle.memoryFraction", "0.3")
                .set("spark.reducer.maxSizeInFlight", "24")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{
                        CategorySortKey.class,
                        IntList.class});
        SparkUtils.setMaster(conf);

        /**
         * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
         * 那个key是需要在进行shuffle的时候，进行网络传输的，因此也是要求实现序列化的
         * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
         * 所以这里要求，为了获取最佳性能，注册一下我们自定义的类
         */

        JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.checkpointFile("hdfs://");
        SQLContext sqlContext = getSQLContext(sc.sc());

        // 生成模拟测试数据
        SparkUtils.mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        args = new String[]{"2"};
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskid);
        if(task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        System.out.println("actionRDD");
        System.out.println(actionRDD.count());
        for(Row row : actionRDD.take(3)){
            System.out.println(row);
        }


        // JavaPairRDD<session_id, Row>  ---  Iterable<Tuple2<session_id, Row>>
        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("sessionid2actionRDD");
        System.out.println(sessionid2actionRDD.count());
        for(Tuple2<String, Row> jpd : sessionid2actionRDD.take(3)){
            System.out.println(jpd);
        }
        // 测试：
        JavaPairRDD<String, Row> sessionid2actionRDD2 = getSessionid2ActionRDD2(actionRDD);
        System.out.println("sessionid2actionRDD2");
        System.out.println(sessionid2actionRDD2.count());
        for(Tuple2<String, Row> jpd : sessionid2actionRDD2.take(3)){
            System.out.println(jpd);
        }

        // <sessionid, fullAggrInfo>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sc, sqlContext, sessionid2actionRDD);
        System.out.println("sessionid2AggrInfoRDD");
        System.out.println(sessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple : sessionid2AggrInfoRDD.take(3)){
            System.out.println(tuple);
        }

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());

        // <sessionid, fullAggrInfo>
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("filteredSessionid2AggrInfoRDD");
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple : filteredSessionid2AggrInfoRDD.take(3)){
            System.out.println(tuple);
        }

        // <sessionid, Row>
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("sessionid2detailRDD");
        System.out.println(sessionid2detailRDD.count());
        for(Tuple2<String, Row> tuple : sessionid2detailRDD.take(6)){
            System.out.println(tuple);
        }

        // 它的 orderCategoryIds.split(",") 和 payCategoryIds.split(",") 逻辑错了，测试
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        Long clickCategoryId = row.getLong(6);
                        if(clickCategoryId != null) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }

                        // 这里的 orderCategoryIds.split(",") 循环估计是做错了
                        String orderCategoryIds = row.getString(8);
                        if(orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            if(orderCategoryIdsSplited.length > 1){
                                System.out.println("orderCategoryIds...");
                            }
                            for(String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                            }
                        }

                        // 这里的 payCategoryIds.split(",") 循环估计是做错了
                        String payCategoryIds = row.getString(10);
                        if(payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            if(payCategoryIdsSplited.length > 1){
                                System.out.println("payCategoryIds...");
                            }
                            for(String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                            }
                        }
                        return list;
                    }
                }
        );
        System.out.println(categoryidRDD.count()); // 原来的一行数据 → 现在list.add变多行了


        // <sessionid, fullAggrInfo>  ---  <sessionid, Row>
//        randomExtractSession(sc, task.getTaskid(), filteredSessionid2AggrInfoRDD, sessionid2detailRDD);

//        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        // 获取top10热门品类
//        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(), sessionid2detailRDD);


        sc.stop();
    }

    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        // 传进来的是迭代器，只执行一次
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                }
                return list;
            }
        });
    }

    public static JavaPairRDD<String, Row> getSessionid2ActionRDD2(JavaRDD<Row> actionRDD) {
        // 传进来的是Row
        return actionRDD.mapToPair( // 如果是 mapPartitionsToPair 则报错
                new PairFunction<Row, String, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }
                }
       );
    }

    private static JavaPairRDD<String, String> aggregateBySession(
            JavaSparkContext sc,
            SQLContext sqlContext,
            JavaPairRDD<String, Row> sessinoid2actionRDD) {
        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD =
                sessionid2ActionsRDD.mapToPair( // <sessionid, Iterable<Row>>

                new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session的访问步长
                        int stepLength = 0;

                        // 遍历session所有的访问行为
                        while(iterator.hasNext()) {
                            // 提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if(userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            // 实际上这里要对数据说明一下
                            // 并不是每一行访问行为都有searchKeyword和clickCategoryId两个字段的
                            // 其实，只有搜索行为，是有searchKeyword字段的
                            // 只有点击品类的行为，是有clickCategoryId字段的
                            // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                            // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                            // 首先要满足：不能是null值
                            // 其次，之前的字符串中还没有搜索词或者点击品类id

                            if(StringUtils.isNotEmpty(searchKeyword)) {
                                if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if(clickCategoryId != null) {
                                if(!clickCategoryIdsBuffer.toString().contains(
                                        String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if(startTime == null) {
                                startTime = actionTime;
                            }
                            if(endTime == null) {
                                endTime = actionTime;
                            }

                            if(actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if(actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            // 计算session访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算session访问时长（秒）
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 大家思考一下
                        // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                        // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                        // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                        // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                        // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        // 然后再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式，还是<sessionid,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 我们这里统一定义，使用key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }

                }
        );
        System.out.println("userid2PartAggrInfoRDD");
        System.out.println(userid2PartAggrInfoRDD.count());
        for(Tuple2<Long, String> jpd : userid2PartAggrInfoRDD.take(3)){
            System.out.println(jpd);
        }

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                }
        );
        System.out.println("userid2InfoRDD");
        System.out.println(userid2InfoRDD.count());
        for(Tuple2<Long, Row> jpd : userid2InfoRDD.take(3)){
            System.out.println(jpd);
        }

        /**
         * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
         * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
         * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
         */
        // 将session粒度聚合数据，与用户信息进行join。
        // JavaPairRDD<Long, Tuple2<String, Row>>中 Long是userid，Tuple2<String, Row>中String是userid2PartAggrInfoRDD中的partAggrInfo，Row是userid2InfoRDD中的Row
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);
        System.out.println("userid2FullInfoRDD");
        System.out.println(userid2FullInfoRDD.count());
        for(Tuple2<Long, Tuple2<String, Row>> jpd : userid2FullInfoRDD.take(3)){
            System.out.println(jpd);
        }


        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }

                }
        );

        /**
         * reduce join转换为map join
         */
//		List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
//		final Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);
//
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2PartAggrInfoRDD.mapToPair(
//
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						// 得到用户信息map
//						List<Tuple2<Long, Row>> userInfos = userInfosBroadcast.value();
//
//						Map<Long, Row> userInfoMap = new HashMap<Long, Row>();
//						for(Tuple2<Long, Row> userInfo : userInfos) {
//							userInfoMap.put(userInfo._1, userInfo._2);
//						}
//
//						// 获取到当前用户对应的信息
//						String partAggrInfo = tuple._2;
//						Row userInfoRow = userInfoMap.get(tuple._1);
//
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//
//				});

        /**
         * sample采样倾斜key单独进行join
         */
//		JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1, 9);
//
//		JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
//
//				new PairFunction<Tuple2<Long,String>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						return new Tuple2<Long, Long>(tuple._1, 1L);
//					}
//
//				});
//
//		JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(
//
//				new Function2<Long, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						return v1 + v2;
//					}
//
//				});
//
//		JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
//
//				new PairFunction<Tuple2<Long,Long>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple)
//							throws Exception {
//						return new Tuple2<Long, Long>(tuple._2, tuple._1);
//					}
//
//				});
//
//		final Long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;
//
//		JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(
//
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return tuple._1.equals(skewedUserid);
//					}
//
//				});
//
//		JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(
//
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return !tuple._1.equals(skewedUserid);
//					}
//
//				});
//
//		JavaPairRDD<String, Row> skewedUserid2infoRDD = userid2InfoRDD.filter(
//
//				new Function<Tuple2<Long,Row>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
//						return tuple._1.equals(skewedUserid);
//					}
//
//				}).flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<Tuple2<String, Row>> call(
//							Tuple2<Long, Row> tuple) throws Exception {
//						Random random = new Random();
//						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//
//						for(int i = 0; i < 100; i++) {
//							int prefix = random.nextInt(100);
//							list.add(new Tuple2<String, Row>(prefix + "_" + tuple._1, tuple._2));
//						}
//
//						return list;
//					}
//
//				});
//
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.mapToPair(
//
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(100);
//						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
//					}
//
//				}).join(skewedUserid2infoRDD).mapToPair(
//
//						new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String, Row>>() {
//
//							private static final long serialVersionUID = 1L;
//
//							@Override
//							public Tuple2<Long, Tuple2<String, Row>> call(
//									Tuple2<String, Tuple2<String, Row>> tuple)
//									throws Exception {
//								long userid = Long.valueOf(tuple._1.split("_")[1]);
//								return new Tuple2<Long, Tuple2<String, Row>>(userid, tuple._2);
//							}
//
//						});
//
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);
//
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);
//
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = joinedRDD.mapToPair(
//
//				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<Long, Tuple2<String, Row>> tuple)
//							throws Exception {
//						String partAggrInfo = tuple._2._1;
//						Row userInfoRow = tuple._2._2;
//
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//
//				});

        /**
         * 使用随机数和扩容表进行join
         */
//		JavaPairRDD<String, Row> expandedRDD = userid2InfoRDD.flatMapToPair(
//
//				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple)
//							throws Exception {
//						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//
//						for(int i = 0; i < 10; i++) {
//							list.add(new Tuple2<String, Row>(0 + "_" + tuple._1, tuple._2));
//						}
//
//						return list;
//					}
//
//				});
//
//		JavaPairRDD<String, String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(
//
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(10);
//						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
//					}
//
//				});
//
//		JavaPairRDD<String, Tuple2<String, Row>> joinedRDD = mappedRDD.join(expandedRDD);
//
//		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
//
//				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<String, Tuple2<String, Row>> tuple)
//							throws Exception {
//						String partAggrInfo = tuple._2._1;
//						Row userInfoRow = tuple._2._2;
//
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//
//				});

        return sessionid2FullAggrInfoRDD;
    }


    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举，其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String,String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if(visitLength >=1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if(visitLength >=4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if(visitLength >=7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if(visitLength >=10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if(visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if(visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if(visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if(visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if(visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if(stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if(stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if(stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if(stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if(stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if(stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                }
        );

        return filteredSessionid2AggrInfoRDD;
    }


    private static JavaPairRDD<String, Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> sessionid2aggrInfoRDD,  // <sessionid, fullAggrInfo>
            JavaPairRDD<String, Row> sessionid2actionRDD) {  // <session_id, Row>
        JavaPairRDD<String, Row> sessionid2detailRDD =
                sessionid2aggrInfoRDD.join(sessionid2actionRDD) // <sessionid, Tuple2<fullAggrInfo, Row>>
                        .mapToPair( // <sessionid, Row>
                                new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
                                    private static final long serialVersionUID = 1L;
                                    @Override
                                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                                    }
                                });
        return sessionid2detailRDD;
    }


    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            // <sessionid, fullAggrInfo>
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            // <sessionid, Row>
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步，计算出每天每小时的session数量：获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
         */
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String,String>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;
                        String startTime = StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);
                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }
                }
        );
        // 按照 yyyy-MM-dd_HH 进行聚合，计算数量
        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         * 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
         */
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

        for(Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }

        // 要抽取100条数据：100/有多少天 = 每天要抽取的数量
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date, Map<hour, List<随机数>>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();

        for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for(long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量，就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int)(((double)count / (double)sessionCount) * extractNumberPerDay);
                if(hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if(extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的  当前小时需要抽取的session数量 的随机数
                for(int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while(extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new HashMap<String, Map<String, IntList>>();

        for(Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();

                IntList fastutilExtractList = new IntArrayList();

                for(int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }

        /**
         * 广播变量，很简单
         * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
         */
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
                sc.broadcast(fastutilDateHourExtractMap);

        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */
        // 执行groupByKey算子，得到<dateHour, Iterable<aggrInfo>>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple)
                            throws Exception {
                        List<Tuple2<String, String>> extractSessionids =
                                new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple._2.iterator();

                        /**
                         * 使用广播变量的时候
                         * 直接调用广播变量（Broadcast类型）的value() / getValue()
                         * 可以获取到之前封装的广播变量
                         */
                        Map<String, Map<String, IntList>> dateHourExtractMap =
                                dateHourExtractMapBroadcast.value();
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        ISessionRandomExtractDAO sessionRandomExtractDAO =
                                DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while(iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();

                            if(extractIndexList.contains(index)) {
                                String sessionid = StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                                // 将数据写入MySQL
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(sessionid);
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                // 将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }

                            index++;
                        }

                        return extractSessionids;
                    }

                });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionidsRDD.join(sessionid2detailRDD);

        extractSessionDetailRDD.foreachPartition(

                new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(
                            Iterator<Tuple2<String, Tuple2<String, Row>>> iterator)
                            throws Exception {
                        List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
                        while(iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                            Row row = tuple._2._2;

                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setTaskid(taskid);
                            sessionDetail.setUserid(row.getLong(1));
                            sessionDetail.setSessionid(row.getString(2));
                            sessionDetail.setPageid(row.getLong(3));
                            sessionDetail.setActionTime(row.getString(4));
                            sessionDetail.setSearchKeyword(row.getString(5));
                            sessionDetail.setClickCategoryId(row.getLong(6));
                            sessionDetail.setClickProductId(row.getLong(7));
                            sessionDetail.setOrderCategoryIds(row.getString(8));
                            sessionDetail.setOrderProductIds(row.getString(9));
                            sessionDetail.setPayCategoryIds(row.getString(10));
                            sessionDetail.setPayProductIds(row.getString(11));

                            sessionDetails.add(sessionDetail);
                        }

                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                        sessionDetailDAO.insertBatch(sessionDetails);
                    }

                });
    }


    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


//    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
//            long taskid,
//            JavaPairRDD<String, Row> sessionid2detailRDD) {
//        /**
//         * 第一步：获取符合条件的session访问过的所有品类
//         */
//
//        // 获取session访问过的所有品类id
//        // 访问过：指的是，点击过、下单过、支付过的品类
//        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
//
//                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Iterable<Tuple2<Long, Long>> call(
//                            Tuple2<String, Row> tuple) throws Exception {
//                        Row row = tuple._2;
//
//                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
//
//                        Long clickCategoryId = row.getLong(6);
//                        if(clickCategoryId != null) {
//                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
//                        }
//
//                        String orderCategoryIds = row.getString(8);
//                        if(orderCategoryIds != null) {
//                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
//                            for(String orderCategoryId : orderCategoryIdsSplited) {
//                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
//                                        Long.valueOf(orderCategoryId)));
//                            }
//                        }
//
//                        String payCategoryIds = row.getString(10);
//                        if(payCategoryIds != null) {
//                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
//                            for(String payCategoryId : payCategoryIdsSplited) {
//                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
//                                        Long.valueOf(payCategoryId)));
//                            }
//                        }
//
//                        return list;
//                    }
//
//                });
//
//        /**
//         * 必须要进行去重
//         * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
//         * 最后很可能会拿到重复的数据
//         */
//        categoryidRDD = categoryidRDD.distinct();
//
//        /**
//         * 第二步：计算各品类的点击、下单和支付的次数
//         */
//
//        // 访问明细中，其中三种访问行为是：点击、下单和支付
//        // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
//        // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
//
//        // 计算各个品类的点击次数
//        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
//                getClickCategoryId2CountRDD(sessionid2detailRDD);
//        // 计算各个品类的下单次数
//        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
//                getOrderCategoryId2CountRDD(sessionid2detailRDD);
//        // 计算各个品类的支付次数
//        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
//                getPayCategoryId2CountRDD(sessionid2detailRDD);
//
//        /**
//         * 第三步：join各品类与它的点击、下单和支付的次数
//         *
//         * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
//         *
//         * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
//         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
//         *
//         * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
//         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
//         * 只不过，没有join到的那个数据，就是0了
//         *
//         */
//        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
//                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
//                payCategoryId2CountRDD);
//
//        /**
//         * 第四步：自定义二次排序key
//         */
//
//        /**
//         * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
//         */
//        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
//
//                new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Tuple2<CategorySortKey, String> call(
//                            Tuple2<Long, String> tuple) throws Exception {
//                        String countInfo = tuple._2;
//                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
//                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
//                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
//                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
//                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
//                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));
//
//                        CategorySortKey sortKey = new CategorySortKey(clickCount,
//                                orderCount, payCount);
//
//                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
//                    }
//
//                });
//
//        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
//                sortKey2countRDD.sortByKey(false);
//
//        /**
//         * 第六步：用take(10)取出top10热门品类，并写入MySQL
//         */
//        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
//
//        List<Tuple2<CategorySortKey, String>> top10CategoryList =
//                sortedCategoryCountRDD.take(10);
//
//        for(Tuple2<CategorySortKey, String> tuple: top10CategoryList) {
//            String countInfo = tuple._2;
//            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
//                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
//            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
//                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
//            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
//                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
//            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
//                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));
//
//            Top10Category category = new Top10Category();
//            category.setTaskid(taskid);
//            category.setCategoryid(categoryid);
//            category.setClickCount(clickCount);
//            category.setOrderCount(orderCount);
//            category.setPayCount(payCount);
//
//            top10CategoryDAO.insert(category);
//        }
//
//        return top10CategoryList;
//    }

}