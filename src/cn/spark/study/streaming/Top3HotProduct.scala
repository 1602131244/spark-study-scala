package cn.spark.study.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{DataTypes,StructField,StructType,StringType,IntegerType}
import org.apache.spark.sql.hive.HiveContext
/**
 * 与spark SQL整合使用
 * 每隔10秒，统计最近60秒大的，每隔种类的每个商品的点击次数，
 * 然后统计出每个种类top3热门的商品
 * GYJ 
 * 2018-2-12
 */
object Top3HotProduct {
  def main(args:Array[String]) : Unit={
    val conf = new SparkConf()
                    .setMaster("local[2]")
                    .setAppName("Top3HotProduct")
    val ssc = new StreamingContext(conf,Seconds(1))
    
    val productClickLogsDStream = ssc.socketTextStream("localhost", 9999)
    
    
    //将日志信息映射为（category_product,1）
    val categoryProductPairDStream = productClickLogsDStream.map(productClickLog => {
      
      val productClickLogSplited = productClickLog.split(" ");
      (productClickLogSplited(2) + "_" + productClickLogSplited(1),1)
    })
    
    
    //执行window操作，每隔十秒，对最近六十秒的数据，执行reduceByKey操作
    
    //计算出来这60秒内，每个种类，每个商品的点击次数
    
    val categoryProductCountsDStream = categoryProductPairDStream.reduceByKeyAndWindow((v1:Int,v2:Int) =>
        v1+v2, 
        Seconds(60), 
        Seconds(10))
        
    
        
    //然后针对60秒内的每个中类的每个商品的点击次数
    //foreachRDD ,在内部使用Spark SQl执行，top3热门商品的统计数据
    
    categoryProductCountsDStream.foreachRDD(categoryProductCountRDD =>{
      
      val categoryProductCountRowRDD = categoryProductCountRDD.map(tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category,product,count)
      })
      
      val structType = StructType(Array(
          StructField("category",StringType,true),
          StructField("product",StringType,true),
          StructField("click_count",IntegerType,true)))
          
          
      val hiveContext = new HiveContext(categoryProductCountRDD.context)
      
      val categoryProductCountDF = hiveContext.createDataFrame(categoryProductCountRowRDD, structType)
      
      categoryProductCountDF.createOrReplaceTempView("product_click_log")
      
      //执行SQl语句
      //针对临时表，统计出每个种类下，点击次数排名签单的热名商品
      
      val top3ProductDF = hiveContext.sql(          
        "SELECT category,product,click_count "
                		+ "FROM ("
                			+ "SELECT "
                				+ "category,"
                				+ "product,"
                				+ "click_count,"
                				+ "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
                		    + "FROM product_click_log"
                		+ ") tmp "
                	    + " where rank <=3"
      
      )
      
      top3ProductDF.show()
      
    })
    
    
    
    
    
    
    
    
    
    ssc.start()
    ssc.awaitTermination()
  }
}