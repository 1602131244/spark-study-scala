package cn.spark.study.core.persistence
import org.apache.spark.{SparkConf,SparkContext}
/**
 * RDD持久化
 * GYJ
 * 2018-1-26
 */
object Persist {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
                   .setAppName("Persist")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C://Users//104515//Desktop//hadoop 测试数据//access_2013_05_30.log", 1).cache()
    //对line执行第一次count操作
    var beginTime = System.currentTimeMillis()
    var counts = lines.count();
    var endTime = System.currentTimeMillis()
    println("cost1 :" + (endTime-beginTime)+ " milliseconds . ")
    println("Text File have " + counts + " lines .")
    
    //对line执行第二次count操作
    beginTime = System.currentTimeMillis()
    counts = lines.count()
    endTime = System.currentTimeMillis()
    println("cost2 :" + (endTime-beginTime)+ " milliseconds . ")
    println("Text File have " + counts + " lines .")
    
    
    
  }
}