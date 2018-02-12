package cn.spark.study.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Durations}
/**
 * 基于transform的实时黑名单过滤
 * GYJ
 * 2018-2-11
 */
object TransformBlacklist {
  def main(args:Array[String]):Unit = {
     val conf = new SparkConf()
                   .setMaster("local[2]")
                   .setAppName("TransformBlacklist")
     val ssc = new StreamingContext(conf,Durations.seconds(5))
     //构造黑名单
     val blackList = Array(("tom",true))
     val blackListRDD = ssc.sparkContext.parallelize(blackList, 5)
     
     val adsClickLogDstream = ssc.socketTextStream("spark1", 9999)
     
     val userAdsClickLogDStream = adsClickLogDstream.map(adsClickLog => (adsClickLog.split(" ")(1),adsClickLog))
     //实时进行黑名单过滤
     val validAdsClickLogDStream = userAdsClickLogDStream.transform(userAdsClickLogRDD => {
       val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD)
       //连接之后，执行filter算子
       val filterRDD = joinedRDD.filter(tuple => {
         //if(tuple._2._2.isDefined && tuple._2._2.get)
         if(tuple._2._2.getOrElse(false)){
           false
         }else{
           true
         }
       })
       val validAdsClickLogRDD = filterRDD.map(tuple => tuple._2._1)
       validAdsClickLogRDD
     })
     
     //打印
     Thread.sleep(5000)
     validAdsClickLogDStream.print
     
     ssc.start()
     ssc.awaitTermination()
  }
}